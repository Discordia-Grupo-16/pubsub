// Package pubsubtest es un bus en memoria con la misma API y la misma
// semántica de reparto que el cliente real, para que los servicios puedan
// testear sus handlers sin levantar RabbitMQ.
//
// Se usa donde el servicio espera un pubsub.Bus:
//
//	bus := pubsubtest.New()
//	require.NoError(t, bus.Subscribe(ctx, spec, projection.Handle))
//	require.NoError(t, bus.Publish(ctx, event))
//	// acá el handler ya corrió: la entrega es sincrónica
//	assert.Empty(t, bus.DeadLettered())
package pubsubtest

import (
	"context"
	"strings"
	"sync"

	"github.com/Discordia-Grupo-16/pubsub"
)

// Bus es un bus en memoria. La entrega es sincrónica: cuando Publish
// vuelve, todos los handlers que correspondían ya corrieron, así que un test
// no necesita esperas ni polling.
type Bus struct {
	// MaxRetries son los reintentos ante un fallo transitorio, sin esperas
	// entre uno y otro. Por defecto cero: un intento y a la dead-letter.
	MaxRetries int

	mu           sync.Mutex
	queues       []*queue
	published    []pubsub.Envelope
	deadLettered []pubsub.Envelope
}

// queue es una cola del bus. Las subscripciones a una misma cola compartida
// se reparten los eventos; cada cola por instancia es su propia cola y
// recibe todo.
type queue struct {
	name        string
	perInstance bool
	consumers   []consumer
	next        int
}

type consumer struct {
	ctx     context.Context
	spec    pubsub.Subscription
	handler pubsub.Handler
}

// New crea un bus vacío.
func New() *Bus { return &Bus{} }

var _ pubsub.Bus = (*Bus)(nil)

// Subscribe registra un consumer. Valida la subscripción igual que el
// cliente real, para que un binding key mal escrito falle en el test y no en
// producción.
func (b *Bus) Subscribe(ctx context.Context, spec pubsub.Subscription, handler pubsub.Handler) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return pubsub.ErrInvalidSubscription
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	target := b.queueFor(spec.Queue)
	target.consumers = append(target.consumers, consumer{ctx: ctx, spec: spec, handler: handler})
	return nil
}

// queueFor devuelve la cola de la subscripción, replicando la diferencia que
// hace el broker: las réplicas de un servicio comparten la cola con nombre y
// compiten por cada evento, mientras que cada cola por instancia es
// independiente y recibe todo.
func (b *Bus) queueFor(spec pubsub.QueueSpec) *queue {
	if !spec.IsPerInstance() {
		for _, q := range b.queues {
			if !q.perInstance && q.name == spec.Name() {
				return q
			}
		}
	}
	created := &queue{name: spec.Name(), perInstance: spec.IsPerInstance()}
	b.queues = append(b.queues, created)
	return created
}

// Publish valida el evento, lo registra y se lo entrega a los consumers que
// lo tengan bindeado.
func (b *Bus) Publish(ctx context.Context, event pubsub.Envelope) error {
	if err := event.Validate(); err != nil {
		return err
	}

	b.mu.Lock()
	b.published = append(b.published, event)
	targets := b.targetsFor(event)
	maxRetries := b.MaxRetries
	b.mu.Unlock()

	for _, target := range targets {
		if err := deliver(ctx, target, event, maxRetries); err != nil {
			b.mu.Lock()
			b.deadLettered = append(b.deadLettered, event)
			b.mu.Unlock()
		}
	}
	return nil
}

// targetsFor elige un consumer por cola: en una cola compartida el evento va
// a uno solo, rotando entre los que haya, igual que el round-robin del
// broker.
func (b *Bus) targetsFor(event pubsub.Envelope) []consumer {
	var targets []consumer

	for _, q := range b.queues {
		var eligible []consumer
		for _, c := range q.consumers {
			if c.ctx.Err() == nil && matchesAny(c.spec.BindingKeys, event.EventType) {
				eligible = append(eligible, c)
			}
		}
		if len(eligible) == 0 {
			continue
		}

		if q.perInstance {
			targets = append(targets, eligible...)
			continue
		}
		targets = append(targets, eligible[q.next%len(eligible)])
		q.next++
	}

	return targets
}

func deliver(ctx context.Context, target consumer, event pubsub.Envelope, maxRetries int) error {
	err := target.handler(ctx, event)
	for attempt := 0; attempt < maxRetries && err != nil && !pubsub.IsPermanent(err); attempt++ {
		err = target.handler(ctx, event)
	}
	return err
}

// Published son todos los eventos publicados, en orden.
func (b *Bus) Published() []pubsub.Envelope {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]pubsub.Envelope(nil), b.published...)
}

// PublishedOf son los eventos publicados de un tipo. Es la forma habitual de
// verificar que una operación emitió el evento que tenía que emitir.
func (b *Bus) PublishedOf(eventType string) []pubsub.Envelope {
	b.mu.Lock()
	defer b.mu.Unlock()

	var found []pubsub.Envelope
	for _, event := range b.published {
		if event.EventType == eventType {
			found = append(found, event)
		}
	}
	return found
}

// DeadLettered son los eventos cuyo handler falló hasta agotar los
// reintentos. En un test que espera que todo ande, tiene que estar vacío.
func (b *Bus) DeadLettered() []pubsub.Envelope {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]pubsub.Envelope(nil), b.deadLettered...)
}

// Reset vacía el bus sin perder las subscripciones.
func (b *Bus) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = nil
	b.deadLettered = nil
}

func matchesAny(bindingKeys []string, eventType string) bool {
	for _, key := range bindingKeys {
		if matchesTopic(strings.Split(key, "."), strings.Split(eventType, ".")) {
			return true
		}
	}
	return false
}

// matchesTopic implementa el matching de un topic exchange: `*` es
// exactamente un segmento y `#` es cero o más.
func matchesTopic(pattern, segments []string) bool {
	if len(pattern) == 0 {
		return len(segments) == 0
	}

	switch pattern[0] {
	case "#":
		for i := 0; i <= len(segments); i++ {
			if matchesTopic(pattern[1:], segments[i:]) {
				return true
			}
		}
		return false
	case "*":
		if len(segments) == 0 {
			return false
		}
		return matchesTopic(pattern[1:], segments[1:])
	default:
		if len(segments) == 0 || segments[0] != pattern[0] {
			return false
		}
		return matchesTopic(pattern[1:], segments[1:])
	}
}
