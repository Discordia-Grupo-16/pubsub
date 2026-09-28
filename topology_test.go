package pubsub

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeChannel registra las declaraciones que le hace el cliente. Verificar
// la topología contra un doble en vez de contra un broker permite que el
// test que protege el riesgo principal —confundir cola compartida con cola
// por instancia— corra siempre, también en la máquina de quien no tiene
// RabbitMQ levantado.
type fakeChannel struct {
	exchanges []declaredExchange
	queues    []declaredQueue
	bindings  []declaredBinding
	qos       []int

	failOn string
}

type declaredExchange struct {
	name, kind          string
	durable, autoDelete bool
	internal, noWait    bool
	args                amqp.Table
}

type declaredQueue struct {
	name                           string
	durable, autoDelete, exclusive bool
	args                           amqp.Table
}

type declaredBinding struct {
	queue, key, exchange string
}

var errBroker = errors.New("broker says no")

func (f *fakeChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	if f.failOn == "exchange:"+name {
		return errBroker
	}
	f.exchanges = append(f.exchanges, declaredExchange{name, kind, durable, autoDelete, internal, noWait, args})
	return nil
}

func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if f.failOn == "queue:"+name {
		return amqp.Queue{}, errBroker
	}
	f.queues = append(f.queues, declaredQueue{name, durable, autoDelete, exclusive, args})
	if name == "" {
		// El broker genera el nombre de las colas anónimas.
		name = "amq.gen-Jz2Kx9"
	}
	return amqp.Queue{Name: name}, nil
}

func (f *fakeChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	if f.failOn == "bind:"+key {
		return errBroker
	}
	f.bindings = append(f.bindings, declaredBinding{name, key, exchange})
	return nil
}

func (f *fakeChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	if f.failOn == "qos" {
		return errBroker
	}
	f.qos = append(f.qos, prefetchCount)
	return nil
}

func testConfig() Config {
	return Config{
		URL:                "amqp://localhost:5672/",
		Exchange:           "discordia.events",
		DeadLetterExchange: "discordia.events.dlx",
		ServiceName:        "chat",
		Prefetch:           16,
		MaxRetries:         3,

		RetryInitialDelay:     200 * time.Millisecond,
		RetryMaxDelay:         5 * time.Second,
		PublishTimeout:        5 * time.Second,
		ReconnectInitialDelay: 500 * time.Millisecond,
		ReconnectMaxDelay:     30 * time.Second,
	}
}

func TestDeclareExchanges_TopicAndDurable(t *testing.T) {
	ch := &fakeChannel{}

	require.NoError(t, declareExchanges(ch, testConfig()))

	require.Len(t, ch.exchanges, 2)
	for _, exchange := range ch.exchanges {
		assert.Equal(t, amqp.ExchangeTopic, exchange.kind, "la routing key es el eventType, hace falta topic")
		assert.True(t, exchange.durable, "un exchange no durable desaparece al reiniciar el broker")
		assert.False(t, exchange.autoDelete)
	}
	assert.Equal(t, "discordia.events", ch.exchanges[0].name)
	assert.Equal(t, "discordia.events.dlx", ch.exchanges[1].name)
}

func TestDeclareExchanges_PropagatesFailures(t *testing.T) {
	for _, target := range []string{"exchange:discordia.events", "exchange:discordia.events.dlx"} {
		err := declareExchanges(&fakeChannel{failOn: target}, testConfig())

		require.ErrorIs(t, err, errBroker)
	}
}

func TestDeclareSubscription_SharedQueueIsDurableAndCompetes(t *testing.T) {
	ch := &fakeChannel{}
	sub := Subscription{
		Queue:       SharedQueue("chat.community-projection"),
		BindingKeys: []string{"community.channel.*", "community.member.joined"},
	}

	name, err := declareSubscription(ch, testConfig(), sub)

	require.NoError(t, err)
	assert.Equal(t, "chat.community-projection", name)

	main := findQueue(t, ch, "chat.community-projection")
	assert.True(t, main.durable, "una proyección no puede perder eventos porque se reinició el broker")
	assert.False(t, main.exclusive, "las réplicas tienen que compartir la cola para repartirse los eventos")
	assert.False(t, main.autoDelete, "la cola sigue acumulando aunque no haya ninguna instancia viva")
	assert.Equal(t, "discordia.events.dlx", main.args["x-dead-letter-exchange"])
	assert.Equal(t, "chat.community-projection.dlq", main.args["x-dead-letter-routing-key"])
}

func TestDeclareSubscription_SharedQueueGetsItsOwnDeadLetterQueue(t *testing.T) {
	ch := &fakeChannel{}
	sub := Subscription{Queue: SharedQueue("chat.community-projection"), BindingKeys: []string{"community.#"}}

	_, err := declareSubscription(ch, testConfig(), sub)

	require.NoError(t, err)
	dlq := findQueue(t, ch, "chat.community-projection.dlq")
	assert.True(t, dlq.durable, "un evento muerto que se pierde al reiniciar no se puede investigar")
	assert.False(t, dlq.autoDelete)
	assert.Contains(t, ch.bindings, declaredBinding{
		queue:    "chat.community-projection.dlq",
		key:      "chat.community-projection.dlq",
		exchange: "discordia.events.dlx",
	})
}

func TestDeclareSubscription_PerInstanceQueueIsExclusiveAndEphemeral(t *testing.T) {
	ch := &fakeChannel{}
	sub := Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.message.sent"}}

	name, err := declareSubscription(ch, testConfig(), sub)

	require.NoError(t, err)
	assert.Equal(t, "amq.gen-Jz2Kx9", name, "el nombre lo genera el broker")

	require.Len(t, ch.queues, 1, "una cola efímera no necesita DLQ: lo que se pierde se recupera del historial")
	queue := ch.queues[0]
	assert.Empty(t, queue.name)
	assert.True(t, queue.exclusive, "sin exclusive, dos instancias compartirían la cola y el fan-out se rompe")
	assert.True(t, queue.autoDelete, "la cola se va con la instancia")
	assert.False(t, queue.durable)
	assert.Nil(t, queue.args, "sin dead-lettering")
}

// Este es el test que protege el riesgo principal de la topología: si el
// fan-out de mensajería se declarara como cola compartida, el mensaje le
// llegaría a una sola instancia y los clientes conectados a las demás no
// verían nada, sin ningún error visible.
func TestDeclareSubscription_TheTwoSemanticsAreNotInterchangeable(t *testing.T) {
	shared := &fakeChannel{}
	perInstance := &fakeChannel{}
	keys := []string{"chat.message.sent"}

	_, err := declareSubscription(shared, testConfig(), Subscription{Queue: SharedQueue("chat.messages"), BindingKeys: keys})
	require.NoError(t, err)
	_, err = declareSubscription(perInstance, testConfig(), Subscription{Queue: PerInstanceQueue(), BindingKeys: keys})
	require.NoError(t, err)

	sharedQueue := findQueue(t, shared, "chat.messages")
	ephemeral := perInstance.queues[0]

	assert.False(t, sharedQueue.exclusive)
	assert.True(t, ephemeral.exclusive)
	assert.NotEqual(t, sharedQueue.autoDelete, ephemeral.autoDelete)
	assert.NotEqual(t, sharedQueue.durable, ephemeral.durable)
}

func TestDeclareSubscription_BindsEveryKeyToTheEventsExchange(t *testing.T) {
	ch := &fakeChannel{}
	sub := Subscription{
		Queue:       PerInstanceQueue(),
		BindingKeys: []string{"chat.message.sent", "chat.message.deleted"},
	}

	_, err := declareSubscription(ch, testConfig(), sub)

	require.NoError(t, err)
	assert.Equal(t, []declaredBinding{
		{queue: "amq.gen-Jz2Kx9", key: "chat.message.sent", exchange: "discordia.events"},
		{queue: "amq.gen-Jz2Kx9", key: "chat.message.deleted", exchange: "discordia.events"},
	}, ch.bindings)
}

func TestDeclareSubscription_Prefetch(t *testing.T) {
	tests := map[string]struct {
		subscriptionPrefetch int
		expected             int
	}{
		"usa el de la config cuando no se pisa": {subscriptionPrefetch: 0, expected: 16},
		"la subscripción puede pisarlo":         {subscriptionPrefetch: 64, expected: 64},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ch := &fakeChannel{}
			sub := Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.#"}, Prefetch: tc.subscriptionPrefetch}

			_, err := declareSubscription(ch, testConfig(), sub)

			require.NoError(t, err)
			assert.Equal(t, []int{tc.expected}, ch.qos)
		})
	}
}

func TestDeclareSubscription_PropagatesFailures(t *testing.T) {
	tests := map[string]struct {
		failOn string
		sub    Subscription
	}{
		"falla la DLQ":                {"queue:chat.projection.dlq", Subscription{Queue: SharedQueue("chat.projection"), BindingKeys: []string{"community.#"}}},
		"falla el bind de la DLQ":     {"bind:chat.projection.dlq", Subscription{Queue: SharedQueue("chat.projection"), BindingKeys: []string{"community.#"}}},
		"falla la cola compartida":    {"queue:chat.projection", Subscription{Queue: SharedQueue("chat.projection"), BindingKeys: []string{"community.#"}}},
		"falla la cola por instancia": {"queue:", Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.#"}}},
		"falla el bind":               {"bind:chat.#", Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.#"}}},
		"falla el prefetch":           {"qos", Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.#"}}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := declareSubscription(&fakeChannel{failOn: tc.failOn}, testConfig(), tc.sub)

			require.ErrorIs(t, err, errBroker)
		})
	}
}

func TestDeclareSubscription_RejectsInvalidSubscription(t *testing.T) {
	ch := &fakeChannel{}

	_, err := declareSubscription(ch, testConfig(), Subscription{BindingKeys: []string{"chat.#"}})

	require.ErrorIs(t, err, ErrInvalidSubscription)
	assert.Empty(t, ch.queues, "no se toca el broker con una subscripción inválida")
}

func TestSubscription_Validate(t *testing.T) {
	tests := map[string]struct {
		sub     Subscription
		wantErr bool
	}{
		"cola compartida con una key":    {Subscription{Queue: SharedQueue("mod.audit"), BindingKeys: []string{"mod.member.banned"}}, false},
		"comodín de un segmento":         {Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"community.member.*"}}, false},
		"comodín de varios segmentos":    {Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"community.#"}}, false},
		"todo el bus":                    {Subscription{Queue: SharedQueue("metrics.all"), BindingKeys: []string{"#"}}, false},
		"prefetch propio":                {Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.#"}, Prefetch: 32}, false},
		"sin semántica de cola":          {Subscription{BindingKeys: []string{"chat.#"}}, true},
		"sin binding keys":               {Subscription{Queue: SharedQueue("chat.q")}, true},
		"binding key vacía":              {Subscription{Queue: SharedQueue("chat.q"), BindingKeys: []string{""}}, true},
		"binding key en mayúsculas":      {Subscription{Queue: SharedQueue("chat.q"), BindingKeys: []string{"Chat.Message.Sent"}}, true},
		"binding key con segmento vacío": {Subscription{Queue: SharedQueue("chat.q"), BindingKeys: []string{"chat..sent"}}, true},
		"prefetch negativo":              {Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.#"}, Prefetch: -1}, true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.sub.Validate()

			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidSubscription)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestQueueSpec_Describes(t *testing.T) {
	assert.Equal(t, "shared queue chat.projection", SharedQueue("chat.projection").String())
	assert.Equal(t, "per-instance queue", PerInstanceQueue().String())
	assert.Equal(t, "chat.projection", SharedQueue("chat.projection").Name())
	assert.Empty(t, PerInstanceQueue().Name())
	assert.True(t, QueueSpec{}.IsZero())
	assert.False(t, SharedQueue("chat.projection").IsZero())
	assert.False(t, PerInstanceQueue().IsZero())
	assert.True(t, PerInstanceQueue().IsPerInstance())
	assert.False(t, SharedQueue("chat.projection").IsPerInstance())
}

func findQueue(t *testing.T, ch *fakeChannel, name string) declaredQueue {
	t.Helper()
	for _, q := range ch.queues {
		if q.name == name {
			return q
		}
	}
	t.Fatalf("no se declaró la cola %q; declaradas: %+v", name, ch.queues)
	return declaredQueue{}
}
