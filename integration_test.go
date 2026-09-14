package pubsub

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Estos tests corren contra un RabbitMQ real y se saltan solos si no hay
// ninguno alcanzable, para que `go test ./...` siga andando en la máquina de
// quien no lo tiene levantado:
//
//	docker compose up -d
//	make test-integration
//
// La cobertura del 70% que pide la DoD se mide con el broker arriba, que es
// como corre CI.

const integrationTimeout = 5 * time.Second

func integrationConfig(t *testing.T) Config {
	t.Helper()

	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://localhost:5672/"
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Skipf("no hay un RabbitMQ alcanzable en %q, se saltan los tests de integración: %v", url, err)
	}
	_ = conn.Close()

	// Cada test usa su propio exchange para no pisarse con los demás ni
	// dejar basura en el broker de quien lo corra en local.
	suffix := uuid.NewString()[:8]
	cfg := Config{
		URL:                   url,
		Exchange:              "discordia.events.test-" + suffix,
		DeadLetterExchange:    "discordia.events.test-" + suffix + ".dlx",
		ServiceName:           "pubsub-itest",
		Prefetch:              8,
		MaxRetries:            2,
		RetryInitialDelay:     10 * time.Millisecond,
		RetryMaxDelay:         50 * time.Millisecond,
		PublishTimeout:        integrationTimeout,
		ReconnectInitialDelay: 50 * time.Millisecond,
		ReconnectMaxDelay:     500 * time.Millisecond,
	}
	require.NoError(t, cfg.Validate())

	t.Cleanup(func() {
		withChannel(t, cfg, func(channel *amqp.Channel) {
			_ = channel.ExchangeDelete(cfg.Exchange, false, false)
			_ = channel.ExchangeDelete(cfg.DeadLetterExchange, false, false)
		})
	})

	return cfg
}

func newTestClient(t *testing.T, cfg Config) *Client {
	t.Helper()

	client, err := Connect(context.Background(), cfg, WithLogger(slog.New(discardHandler())))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

// sharedQueueFor devuelve una cola compartida con nombre único para el test
// y se encarga de borrarla —y a su DLQ— al terminar.
func sharedQueueFor(t *testing.T, cfg Config) QueueSpec {
	t.Helper()

	name := "itest." + uuid.NewString()[:8]
	t.Cleanup(func() {
		withChannel(t, cfg, func(channel *amqp.Channel) {
			_, _ = channel.QueueDelete(name, false, false, false)
			_, _ = channel.QueueDelete(name+deadLetterQueueSuffix, false, false, false)
		})
	})

	return SharedQueue(name)
}

func withChannel(t *testing.T, cfg Config, fn func(*amqp.Channel)) {
	t.Helper()

	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	channel, err := conn.Channel()
	if err != nil {
		return
	}
	defer func() { _ = channel.Close() }()

	fn(channel)
}

// received es un buzón para que los handlers de los tests dejen lo que les
// llega sin carreras.
type received struct {
	mu     sync.Mutex
	events []Envelope
}

func (r *received) handler() Handler {
	return func(_ context.Context, event Envelope) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.events = append(r.events, event)
		return nil
	}
}

func (r *received) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func (r *received) all() []Envelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Envelope(nil), r.events...)
}

// waitFor espera hasta que se cumpla la condición o se acabe el tiempo.
func waitFor(t *testing.T, reason string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(integrationTimeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("se acabó el tiempo esperando: %s", reason)
}

func testMessage(t *testing.T) Envelope {
	t.Helper()

	event, err := NewEnvelope("chat.message.sent", "chat", map[string]string{"messageId": uuid.NewString()})
	require.NoError(t, err)
	return event
}

func TestIntegration_PublishAndConsume(t *testing.T) {
	cfg := integrationConfig(t)
	client := newTestClient(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var inbox received
	require.NoError(t, client.Subscribe(ctx, Subscription{
		Queue:       sharedQueueFor(t, cfg),
		BindingKeys: []string{"chat.message.sent"},
	}, inbox.handler()))

	sent := testMessage(t)
	require.NoError(t, client.Publish(ctx, sent))

	waitFor(t, "que llegue el evento", func() bool { return inbox.count() == 1 })

	got := inbox.all()[0]
	assert.Equal(t, sent.EventID, got.EventID)
	assert.Equal(t, sent.CorrelationID, got.CorrelationID)
	assert.Equal(t, sent.EventType, got.EventType)
	assert.Equal(t, sent.OccurredAt, got.OccurredAt, "occurredAt sobrevive el viaje: es la base para resolver el desorden")
	assert.JSONEq(t, string(sent.Data), string(got.Data))
}

func TestIntegration_BindingKeysFilterWhatArrives(t *testing.T) {
	cfg := integrationConfig(t)
	client := newTestClient(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var inbox received
	require.NoError(t, client.Subscribe(ctx, Subscription{
		Queue:       sharedQueueFor(t, cfg),
		BindingKeys: []string{"community.member.*"},
	}, inbox.handler()))

	ignored, err := NewEnvelope("chat.message.sent", "chat", map[string]string{"messageId": "m1"})
	require.NoError(t, err)
	wanted, err := NewEnvelope("community.member.joined", "community", map[string]string{"userId": "u1"})
	require.NoError(t, err)

	require.NoError(t, client.Publish(ctx, ignored))
	require.NoError(t, client.Publish(ctx, wanted))

	waitFor(t, "que llegue el evento bindeado", func() bool { return inbox.count() == 1 })
	time.Sleep(200 * time.Millisecond)

	require.Len(t, inbox.all(), 1)
	assert.Equal(t, "community.member.joined", inbox.all()[0].EventType)
}

// Esta es la propiedad que el CP1 tiene que demostrar: dos instancias del
// servicio de mensajería, cada una con sus propios WebSockets conectados,
// tienen que recibir las dos el mismo mensaje.
func TestIntegration_PerInstanceQueuesFanOutToEveryInstance(t *testing.T) {
	cfg := integrationConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := newTestClient(t, cfg)
	second := newTestClient(t, cfg)

	var firstInbox, secondInbox received
	spec := Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.message.sent"}}
	require.NoError(t, first.Subscribe(ctx, spec, firstInbox.handler()))
	require.NoError(t, second.Subscribe(ctx, spec, secondInbox.handler()))

	sent := testMessage(t)
	require.NoError(t, first.Publish(ctx, sent))

	waitFor(t, "que el mensaje llegue a las dos instancias", func() bool {
		return firstInbox.count() == 1 && secondInbox.count() == 1
	})
	assert.Equal(t, sent.EventID, firstInbox.all()[0].EventID)
	assert.Equal(t, sent.EventID, secondInbox.all()[0].EventID)
}

// La contracara: sobre una cola compartida, el mismo evento lo procesa una
// sola réplica. Es lo que hace que una proyección no se aplique N veces.
func TestIntegration_SharedQueueSplitsEventsBetweenReplicas(t *testing.T) {
	cfg := integrationConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := newTestClient(t, cfg)
	second := newTestClient(t, cfg)

	spec := Subscription{Queue: sharedQueueFor(t, cfg), BindingKeys: []string{"chat.message.sent"}}
	var firstInbox, secondInbox received
	require.NoError(t, first.Subscribe(ctx, spec, firstInbox.handler()))
	require.NoError(t, second.Subscribe(ctx, spec, secondInbox.handler()))

	const total = 10
	ids := map[string]struct{}{}
	for i := 0; i < total; i++ {
		event := testMessage(t)
		ids[event.EventID] = struct{}{}
		require.NoError(t, first.Publish(ctx, event))
	}

	waitFor(t, "que se procesen todos los eventos", func() bool {
		return firstInbox.count()+secondInbox.count() == total
	})
	time.Sleep(200 * time.Millisecond)

	all := append(firstInbox.all(), secondInbox.all()...)
	require.Len(t, all, total, "ninguna réplica procesa un evento que ya procesó la otra")
	for _, event := range all {
		_, known := ids[event.EventID]
		assert.True(t, known)
		delete(ids, event.EventID)
	}
	assert.Empty(t, ids, "no se perdió ninguno")
	assert.Positive(t, firstInbox.count())
	assert.Positive(t, secondInbox.count())
}

func TestIntegration_TransientFailureIsRetriedAndThenAcked(t *testing.T) {
	cfg := integrationConfig(t)
	client := newTestClient(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := sharedQueueFor(t, cfg)
	var mu sync.Mutex
	attempts := 0
	require.NoError(t, client.Subscribe(ctx, Subscription{
		Queue:       queue,
		BindingKeys: []string{"chat.message.sent"},
	}, func(context.Context, Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts < 3 {
			return errTransient
		}
		return nil
	}))

	require.NoError(t, client.Publish(ctx, testMessage(t)))

	waitFor(t, "que el handler termine bien después de reintentar", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts == 3
	})

	assert.Empty(t, drain(t, cfg, queue.Name()+deadLetterQueueSuffix),
		"un fallo transitorio que después se resuelve no deja nada en la DLQ")
}

func TestIntegration_PermanentFailureGoesToTheDeadLetterQueue(t *testing.T) {
	cfg := integrationConfig(t)
	client := newTestClient(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := sharedQueueFor(t, cfg)
	var mu sync.Mutex
	attempts := 0
	require.NoError(t, client.Subscribe(ctx, Subscription{
		Queue:       queue,
		BindingKeys: []string{"chat.message.sent"},
	}, func(context.Context, Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		return Permanent(errBusiness)
	}))

	sent := testMessage(t)
	require.NoError(t, client.Publish(ctx, sent))

	var dead [][]byte
	waitFor(t, "que el evento aparezca en la DLQ", func() bool {
		dead = drain(t, cfg, queue.Name()+deadLetterQueueSuffix)
		return len(dead) == 1
	})

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, attempts, "un fallo permanente no se reintenta")

	var deadEvent Envelope
	require.NoError(t, json.Unmarshal(dead[0], &deadEvent))
	assert.Equal(t, sent.EventID, deadEvent.EventID, "el evento muerto queda entero para poder investigarlo")
}

func TestIntegration_MalformedMessageIsDeadLettered(t *testing.T) {
	cfg := integrationConfig(t)
	client := newTestClient(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := sharedQueueFor(t, cfg)
	var inbox received
	require.NoError(t, client.Subscribe(ctx, Subscription{
		Queue:       queue,
		BindingKeys: []string{"chat.message.sent"},
	}, inbox.handler()))

	// Un mensaje que no es un envelope, publicado a mano: lo que pasaría si
	// alguien publicara al bus sin usar este cliente.
	withChannel(t, cfg, func(channel *amqp.Channel) {
		require.NoError(t, channel.PublishWithContext(ctx, cfg.Exchange, "chat.message.sent", false, false,
			amqp.Publishing{ContentType: "application/json", Body: []byte(`{"no soy":"un evento"}`)}))
	})

	var dead [][]byte
	waitFor(t, "que el mensaje malformado aparezca en la DLQ", func() bool {
		dead = drain(t, cfg, queue.Name()+deadLetterQueueSuffix)
		return len(dead) == 1
	})
	assert.Zero(t, inbox.count(), "el handler nunca ve un mensaje que no cumple el contrato")
}

func TestIntegration_ReconnectsAfterConnectionLoss(t *testing.T) {
	cfg := integrationConfig(t)
	client := newTestClient(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var inbox received
	require.NoError(t, client.Subscribe(ctx, Subscription{
		Queue:       sharedQueueFor(t, cfg),
		BindingKeys: []string{"chat.message.sent"},
	}, inbox.handler()))
	require.NoError(t, client.Publish(ctx, testMessage(t)))
	waitFor(t, "que llegue el primer evento", func() bool { return inbox.count() == 1 })

	// Se corta la conexión por debajo, como si se hubiera reiniciado el
	// broker.
	client.mu.Lock()
	conn := client.conn
	client.mu.Unlock()
	require.NoError(t, conn.Close())

	waitFor(t, "que el cliente vuelva a publicar", func() bool {
		return client.Publish(ctx, testMessage(t)) == nil
	})
	waitFor(t, "que la subscripción se haya rehecho sola", func() bool { return inbox.count() >= 2 })
}

func TestIntegration_PublishFailsAfterClose(t *testing.T) {
	cfg := integrationConfig(t)
	client, err := Connect(context.Background(), cfg, WithLogger(slog.New(discardHandler())))
	require.NoError(t, err)
	require.NoError(t, client.Close())

	err = client.Publish(context.Background(), testMessage(t))

	assert.ErrorIs(t, err, ErrClosed)
}

// drain saca todo lo que haya en una cola sin dejarlo ahí.
func drain(t *testing.T, cfg Config, queue string) [][]byte {
	t.Helper()

	var bodies [][]byte
	withChannel(t, cfg, func(channel *amqp.Channel) {
		for {
			delivery, ok, err := channel.Get(queue, true)
			if err != nil || !ok {
				return
			}
			bodies = append(bodies, delivery.Body)
		}
	})
	return bodies
}
