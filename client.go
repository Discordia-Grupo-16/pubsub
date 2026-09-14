package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrClosed indica que el cliente ya se cerró y no acepta más operaciones.
var ErrClosed = errors.New("pubsub client is closed")

// Client es la conexión de un servicio al bus. Uno por proceso: mantiene una
// única conexión AMQP, un canal para publicar y un canal por subscripción, y
// se reconecta solo si el broker se cae.
type Client struct {
	cfg    Config
	logger *slog.Logger
	dial   dialFunc

	mu        sync.Mutex
	conn      *amqp.Connection
	publishCh *amqp.Channel
	subs      []*subscription
	closed    bool

	shutdown chan struct{}
	wg       sync.WaitGroup
}

// dialFunc abre la conexión al broker. Es un campo para poder inyectar
// fallos de conexión en los tests.
type dialFunc func(url string, cfg amqp.Config) (*amqp.Connection, error)

// ClientOption ajusta el cliente al construirlo.
type ClientOption func(*Client)

// WithLogger reemplaza el logger. Por defecto usa slog.Default().
//
// El cliente loguea el tipo de evento, sus identificadores y el resultado,
// nunca el payload: por ahí pasan mensajes privados y datos personales, que
// no pueden terminar en un log.
func WithLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// Connect abre la conexión al broker, declara los exchanges y deja el
// cliente listo para publicar y subscribirse. Falla si el broker no está
// disponible: un servicio que arranca sin bus es un servicio que va a fallar
// en silencio en la primera operación.
func Connect(ctx context.Context, cfg Config, opts ...ClientOption) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client := &Client{
		cfg:      cfg,
		logger:   slog.Default(),
		dial:     amqp.DialConfig,
		shutdown: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(client)
	}

	if err := client.connect(ctx); err != nil {
		return nil, err
	}

	client.wg.Add(1)
	go func() {
		defer client.wg.Done()
		client.supervise()
	}()

	return client, nil
}

// connect abre la conexión y el canal de publicación, y declara los
// exchanges. Se usa tanto al arrancar como en cada reconexión.
func (c *Client) connect(ctx context.Context) error {
	properties := amqp.NewConnectionProperties()
	// El nombre de la conexión es lo que hace legible la management UI
	// cuando hay ocho servicios conectados al mismo broker.
	properties.SetClientConnectionName(c.cfg.ServiceName)

	conn, err := c.dial(c.cfg.URL, amqp.Config{Properties: properties})
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open publish channel: %w", err)
	}

	// Sin confirms, Publish devuelve nil apenas escribe en el socket y un
	// evento que el broker rechaza se pierde sin que nadie se entere.
	if err := channel.Confirm(false); err != nil {
		_ = conn.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}

	if err := declareExchanges(channel, c.cfg); err != nil {
		_ = conn.Close()
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.publishCh = channel
	c.mu.Unlock()

	_ = ctx // la conexión de amqp091 no toma contexto; queda por simetría de la API

	return nil
}

// Publish manda un evento al topic exchange usando su eventType como routing
// key, y espera la confirmación del broker.
//
// No usa el flag mandatory: que todavía no haya ningún consumer atado a ese
// evento es normal en pub/sub y no es un error del publicador.
func (c *Client) Publish(ctx context.Context, event Envelope) error {
	if err := event.Validate(); err != nil {
		return err
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", event.EventType, err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.cfg.PublishTimeout)
	defer cancel()

	c.mu.Lock()
	if c.closed || c.publishCh == nil {
		c.mu.Unlock()
		return ErrClosed
	}
	// La espera de la confirmación queda fuera del lock: si no, los
	// publishes de todo el proceso se serializarían con la latencia del
	// broker.
	confirmation, err := c.publishCh.PublishWithDeferredConfirmWithContext(
		ctx, c.cfg.Exchange, event.RoutingKey(), false, false, publishing(event, body),
	)
	c.mu.Unlock()

	if err != nil {
		return fmt.Errorf("publish %s: %w", event.EventType, err)
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait confirmation for %s: %w", event.EventType, err)
	}
	if !acked {
		return fmt.Errorf("broker rejected %s (eventId %s)", event.EventType, event.EventID)
	}

	c.logger.Debug("event published",
		"eventType", event.EventType,
		"eventId", event.EventID,
		"correlationId", event.CorrelationID,
	)
	return nil
}

// publishing arma el mensaje AMQP. Los identificadores del sobre se repiten
// en las propiedades del mensaje para que se vean en la management UI sin
// tener que abrir el body.
func publishing(event Envelope, body []byte) amqp.Publishing {
	return amqp.Publishing{
		MessageId:     event.EventID,
		CorrelationId: event.CorrelationID,
		Type:          event.EventType,
		AppId:         event.Producer,
		Timestamp:     event.OccurredAt,
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		Body:          body,
	}
}

// supervise vigila la conexión y la rehace cuando se cae. Sin esto, un
// reinicio del broker deja al servicio corriendo pero mudo y sordo, que es
// peor que caerse.
func (c *Client) supervise() {
	for {
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()
		if conn == nil {
			return
		}

		closed := conn.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case <-c.shutdown:
			return
		case reason, ok := <-closed:
			if !ok {
				return
			}
			select {
			case <-c.shutdown:
				return
			default:
			}
			c.logger.Warn("broker connection lost, reconnecting", "reason", reason)
			if !c.reconnect() {
				return
			}
		}
	}
}

// reconnect reintenta la conexión con backoff hasta lograrlo o hasta que el
// cliente se cierre. Devuelve false si hay que dejar de intentar.
func (c *Client) reconnect() bool {
	policy := retryPolicy{
		maxRetries: 0,
		initial:    c.cfg.ReconnectInitialDelay,
		max:        c.cfg.ReconnectMaxDelay,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-c.shutdown
		cancel()
	}()

	for attempt := 1; ; attempt++ {
		if err := policy.wait(ctx, attempt); err != nil {
			return false
		}

		if err := c.connect(ctx); err != nil {
			c.logger.Warn("reconnect failed", "attempt", attempt, "error", err)
			continue
		}
		if err := c.restartSubscriptions(); err != nil {
			c.logger.Warn("resubscribe failed", "attempt", attempt, "error", err)
			continue
		}

		c.logger.Info("reconnected to broker", "attempt", attempt)
		return true
	}
}

// Close corta las subscripciones y cierra la conexión. Es idempotente.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.mu.Unlock()

	close(c.shutdown)
	c.wg.Wait()

	if conn != nil {
		if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			return fmt.Errorf("close broker connection: %w", err)
		}
	}
	return nil
}
