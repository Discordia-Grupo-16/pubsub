package pubsub

import (
	"errors"
	"fmt"
	"regexp"

	amqp "github.com/rabbitmq/amqp091-go"
)

// deadLetterQueueSuffix nombra la DLQ de una cola compartida a partir de la
// cola misma, para que en la management UI queden una al lado de la otra.
const deadLetterQueueSuffix = ".dlq"

// bindingKeyPattern acepta las routing keys del contrato de eventos y los
// comodines de un topic exchange: `*` (un segmento) y `#` (cero o más).
var bindingKeyPattern = regexp.MustCompile(`^(?:[a-z][a-z0-9-]*|\*|#)(?:\.(?:[a-z][a-z0-9-]*|\*|#))*$`)

// QueueSpec elige con qué semántica se declara la cola de una subscripción.
// No tiene valor por defecto a propósito: es la decisión que hay que tomar
// explícitamente en cada consumer, porque equivocarla no da error, solo hace
// que a algunos usuarios no les llegue nada.
type QueueSpec struct {
	name        string
	perInstance bool
}

// SharedQueue declara una cola durable con nombre fijo, compartida por todas
// las réplicas del servicio: el broker las reparte round-robin y cada evento
// lo procesa una sola.
//
// Es lo que corresponde a todo lo que termina escribiendo en la base del
// servicio —proyecciones, contadores, auditoría—: no tiene sentido que tres
// instancias apliquen el mismo evento tres veces.
func SharedQueue(name string) QueueSpec {
	return QueueSpec{name: name}
}

// PerInstanceQueue declara una cola exclusive y auto-delete con nombre
// generado por el broker: cada instancia del servicio recibe *todos* los
// eventos, y la cola desaparece cuando la instancia se va.
//
// Es el único modo que hace funcionar el fan-out multiinstancia. Corresponde
// cuando lo que hay que hacer con el evento depende del estado local de esa
// instancia — típicamente reenviárselo a los WebSockets que tiene conectados.
func PerInstanceQueue() QueueSpec {
	return QueueSpec{perInstance: true}
}

// IsZero indica que no se eligió ninguna semántica de cola.
func (q QueueSpec) IsZero() bool { return q.name == "" && !q.perInstance }

// IsPerInstance indica si cada instancia recibe todos los eventos.
func (q QueueSpec) IsPerInstance() bool { return q.perInstance }

// Name es el nombre de la cola compartida. Vacío para una cola por
// instancia: en ese caso el nombre lo genera el broker al declararla.
func (q QueueSpec) Name() string { return q.name }

func (q QueueSpec) String() string {
	if q.perInstance {
		return "per-instance queue"
	}
	return "shared queue " + q.name
}

// Subscription describe qué eventos consume un servicio y con qué semántica.
type Subscription struct {
	// Queue es la semántica de cola: SharedQueue o PerInstanceQueue.
	Queue QueueSpec

	// BindingKeys son los patrones de routing key que la cola recibe, con
	// los comodines del topic exchange (`community.member.*`,
	// `community.#`, `chat.message.sent`).
	BindingKeys []string

	// Prefetch pisa el prefetch global para esta subscripción. En cero usa
	// el de la configuración.
	Prefetch int
}

// ErrInvalidSubscription encabeza todo error de definición de una
// subscripción.
var ErrInvalidSubscription = errors.New("invalid subscription")

// Validate verifica la subscripción antes de tocar el broker.
func (s Subscription) Validate() error {
	if s.Queue.IsZero() {
		return fmt.Errorf("%w: Queue is required, use SharedQueue(name) or PerInstanceQueue()", ErrInvalidSubscription)
	}
	if len(s.BindingKeys) == 0 {
		return fmt.Errorf("%w: at least one binding key is required, a queue with no bindings never receives anything", ErrInvalidSubscription)
	}
	for _, key := range s.BindingKeys {
		if !bindingKeyPattern.MatchString(key) {
			return fmt.Errorf("%w: binding key %q is not a valid topic pattern", ErrInvalidSubscription, key)
		}
	}
	if s.Prefetch < 0 {
		return fmt.Errorf("%w: Prefetch must be >= 0, got %d", ErrInvalidSubscription, s.Prefetch)
	}
	return nil
}

// prefetch resuelve el prefetch efectivo de la subscripción.
func (s Subscription) prefetch(cfg Config) int {
	if s.Prefetch > 0 {
		return s.Prefetch
	}
	return cfg.Prefetch
}

// amqpChannel es la porción del canal de AMQP que usa el cliente. Existe
// para poder verificar la topología —que es donde vive el riesgo— sin
// depender de un broker levantado.
type amqpChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
}

// declareExchanges declara el topic exchange de eventos y su dead-letter
// exchange. Es idempotente: declarar un exchange que ya existe con los
// mismos parámetros no hace nada.
func declareExchanges(ch amqpChannel, cfg Config) error {
	if err := ch.ExchangeDeclare(cfg.Exchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", cfg.Exchange, err)
	}
	if err := ch.ExchangeDeclare(cfg.DeadLetterExchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead-letter exchange %s: %w", cfg.DeadLetterExchange, err)
	}
	return nil
}

// declareSubscription declara la cola de la subscripción con la semántica
// que corresponda, la ata al exchange por cada binding key y fija el
// prefetch. Devuelve el nombre efectivo de la cola, que para una cola por
// instancia lo genera el broker.
func declareSubscription(ch amqpChannel, cfg Config, sub Subscription) (string, error) {
	if err := sub.Validate(); err != nil {
		return "", err
	}

	var queue amqp.Queue
	var err error

	if sub.Queue.IsPerInstance() {
		// Sin durabilidad ni dead-lettering: si la instancia muere, su cola
		// se va con ella. Lo que consume una cola por instancia es fan-out
		// en vivo, y la pérdida es recuperable por otro lado (el historial
		// persistido), así que acumular eventos muertos de instancias que ya
		// no existen sería basura, no resiliencia.
		queue, err = ch.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			return "", fmt.Errorf("declare per-instance queue: %w", err)
		}
	} else {
		deadLetterQueue := sub.Queue.Name() + deadLetterQueueSuffix
		if _, err = ch.QueueDeclare(deadLetterQueue, true, false, false, false, nil); err != nil {
			return "", fmt.Errorf("declare dead-letter queue %s: %w", deadLetterQueue, err)
		}
		if err = ch.QueueBind(deadLetterQueue, deadLetterQueue, cfg.DeadLetterExchange, false, nil); err != nil {
			return "", fmt.Errorf("bind dead-letter queue %s: %w", deadLetterQueue, err)
		}

		queue, err = ch.QueueDeclare(sub.Queue.Name(), true, false, false, false, amqp.Table{
			"x-dead-letter-exchange":    cfg.DeadLetterExchange,
			"x-dead-letter-routing-key": deadLetterQueue,
		})
		if err != nil {
			return "", fmt.Errorf("declare shared queue %s: %w", sub.Queue.Name(), err)
		}
	}

	for _, key := range sub.BindingKeys {
		if err := ch.QueueBind(queue.Name, key, cfg.Exchange, false, nil); err != nil {
			return "", fmt.Errorf("bind queue %s to %s: %w", queue.Name, key, err)
		}
	}

	if err := ch.Qos(sub.prefetch(cfg), 0, false); err != nil {
		return "", fmt.Errorf("set prefetch on queue %s: %w", queue.Name, err)
	}

	return queue.Name, nil
}
