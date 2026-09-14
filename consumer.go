package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler procesa un evento recibido del bus.
//
// El bus entrega at-least-once: todo handler puede recibir el mismo evento
// más de una vez y tiene que tolerarlo. La idempotencia es responsabilidad
// del handler —por eventId, por clave de negocio o comparando occurredAt
// contra el estado guardado—; el cliente no la puede resolver por él.
//
// El valor de retorno decide qué pasa con el mensaje:
//
//   - nil: se ackea y sale de la cola.
//   - un error cualquiera: se toma como fallo transitorio y se reintenta con
//     backoff; agotados los reintentos, va a la dead-letter queue.
//   - Permanent(err): va a la dead-letter queue sin reintentar.
type Handler func(ctx context.Context, event Envelope) error

// subscription es una subscripción ya registrada: se guarda para poder
// rehacerla tal cual después de una reconexión.
type subscription struct {
	spec    Subscription
	handler Handler
	ctx     context.Context
}

// Subscribe registra un consumer y lo deja corriendo en segundo plano.
// Devuelve error solo si la subscripción no se pudo establecer; los fallos
// de cada evento los resuelve el ciclo de reintentos.
//
// La subscripción vive hasta que se cancela ctx o se cierra el cliente, y se
// rehace sola si el broker se cae.
func (c *Client) Subscribe(ctx context.Context, spec Subscription, handler Handler) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("%w: handler is required", ErrInvalidSubscription)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.mu.Unlock()

	sub := &subscription{spec: spec, handler: handler, ctx: ctx}
	if err := c.startSubscription(sub); err != nil {
		return err
	}

	c.mu.Lock()
	c.subs = append(c.subs, sub)
	c.mu.Unlock()

	return nil
}

// startSubscription declara la topología de la subscripción sobre un canal
// propio y arranca el loop de consumo.
//
// Cada subscripción usa su propio canal: el prefetch se configura por canal,
// así que compartirlo mezclaría el límite de dos consumers con ritmos
// distintos.
func (c *Client) startSubscription(sub *subscription) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return ErrClosed
	}

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel for %s: %w", sub.spec.Queue, err)
	}

	queue, err := declareSubscription(channel, c.cfg, sub.spec)
	if err != nil {
		_ = channel.Close()
		return err
	}

	// autoAck en false: el ack va después de procesar. Con autoAck, un
	// evento se da por procesado apenas se entrega y se pierde si el
	// proceso se cae a mitad de camino.
	deliveries, err := channel.Consume(
		queue,
		fmt.Sprintf("%s-%s", c.cfg.ServiceName, uuid.NewString()[:8]),
		false,
		sub.spec.Queue.IsPerInstance(),
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		return fmt.Errorf("consume from %s: %w", queue, err)
	}

	c.logger.Info("subscribed",
		"queue", queue,
		"mode", sub.spec.Queue.String(),
		"bindingKeys", sub.spec.BindingKeys,
		"prefetch", sub.spec.prefetch(c.cfg),
	)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consume(sub, channel, queue, deliveries)
	}()

	return nil
}

// restartSubscriptions rehace las subscripciones vivas después de una
// reconexión. Las que ya tenían su contexto cancelado no se rehacen.
func (c *Client) restartSubscriptions() error {
	c.mu.Lock()
	subs := make([]*subscription, len(c.subs))
	copy(subs, c.subs)
	c.mu.Unlock()

	for _, sub := range subs {
		if sub.ctx.Err() != nil {
			continue
		}
		if err := c.startSubscription(sub); err != nil {
			return err
		}
	}
	return nil
}

// consume procesa los mensajes de una cola de a uno. Secuencial a propósito:
// el orden ya es débil en el bus, y procesar en paralelo dentro de una misma
// cola lo rompería del todo sin necesidad. Para más paralelismo se corren
// más instancias del servicio, que es el modelo que la cola compartida ya
// soporta.
func (c *Client) consume(sub *subscription, channel *amqp.Channel, queue string, deliveries <-chan amqp.Delivery) {
	ctx, cancel := c.subscriptionContext(sub.ctx)
	defer cancel()
	defer func() { _ = channel.Close() }()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("subscription stopped", "queue", queue)
			return
		case delivery, ok := <-deliveries:
			if !ok {
				// El broker cerró la entrega: o se cayó la conexión —y el
				// supervisor va a rehacer la subscripción— o se está
				// cerrando el cliente.
				return
			}
			c.handleDelivery(ctx, sub, queue, delivery)
		}
	}
}

// subscriptionContext ata el contexto de la subscripción al cierre del
// cliente, para que un shutdown corte también el backoff de un evento en
// vuelo.
func (c *Client) subscriptionContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-c.shutdown:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// handleDelivery decodifica el mensaje, se lo pasa al handler con su ciclo
// de reintentos y resuelve el ack según el resultado.
func (c *Client) handleDelivery(ctx context.Context, sub *subscription, queue string, delivery amqp.Delivery) {
	var event Envelope
	if err := json.Unmarshal(delivery.Body, &event); err != nil {
		c.deadLetter(queue, delivery, "message is not a valid envelope", err)
		return
	}
	if err := event.Validate(); err != nil {
		c.deadLetter(queue, delivery, "envelope does not meet the contract", err)
		return
	}

	err := runWithRetries(ctx, newRetryPolicy(c.cfg), sub.handler, event)

	switch decideAck(err) {
	case ackEvent:
		if ackErr := delivery.Ack(false); ackErr != nil {
			c.logger.Error("ack failed", "queue", queue, "eventId", event.EventID, "error", ackErr)
		}
	case requeueEvent:
		// No se llegó a procesar: vuelve a la cola para que lo tome otra
		// instancia o esta misma después de reiniciar.
		c.logger.Warn("event requeued", "queue", queue, "eventType", event.EventType, "eventId", event.EventID, "error", err)
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			c.logger.Error("nack failed", "queue", queue, "eventId", event.EventID, "error", nackErr)
		}
	case deadLetterEvent:
		c.logger.Error("event dead-lettered",
			"queue", queue,
			"eventType", event.EventType,
			"eventId", event.EventID,
			"correlationId", event.CorrelationID,
			"permanent", IsPermanent(err),
			"error", err,
		)
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			c.logger.Error("nack failed", "queue", queue, "eventId", event.EventID, "error", nackErr)
		}
	}
}

// deadLetter saca de la cola un mensaje que nunca va a poder procesarse.
// Reintentarlo no cambia nada y mantenerlo ahí tapa la cola.
func (c *Client) deadLetter(queue string, delivery amqp.Delivery, reason string, err error) {
	c.logger.Error("message dead-lettered",
		"queue", queue,
		"reason", reason,
		"messageId", delivery.MessageId,
		"routingKey", delivery.RoutingKey,
		"error", err,
	)
	if nackErr := delivery.Nack(false, false); nackErr != nil {
		c.logger.Error("nack failed", "queue", queue, "messageId", delivery.MessageId, "error", nackErr)
	}
}

// runWithRetries ejecuta el handler y lo reintenta con backoff mientras el
// fallo sea transitorio.
func runWithRetries(ctx context.Context, policy retryPolicy, handler Handler, event Envelope) error {
	err := handler(ctx, event)
	if err == nil || IsPermanent(err) {
		return err
	}

	for attempt := 1; attempt <= policy.maxRetries; attempt++ {
		if waitErr := policy.wait(ctx, attempt); waitErr != nil {
			return waitErr
		}

		err = handler(ctx, event)
		if err == nil || IsPermanent(err) {
			return err
		}
	}

	return err
}

// ackDecision es qué hacer con un mensaje según cómo terminó su handler.
type ackDecision int

const (
	// ackEvent: procesado con éxito, sale de la cola.
	ackEvent ackDecision = iota
	// deadLetterEvent: no se pudo procesar y no tiene sentido insistir.
	deadLetterEvent
	// requeueEvent: no se llegó a procesar, vuelve a la cola.
	requeueEvent
)

func decideAck(err error) ackDecision {
	switch {
	case err == nil:
		return ackEvent
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Se cortó por un shutdown o por un timeout, no porque el evento
		// esté mal: devolverlo a la cola es lo correcto.
		return requeueEvent
	default:
		return deadLetterEvent
	}
}
