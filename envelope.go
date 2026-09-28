// Package pubsub es el cliente compartido de Pub/Sub de Discordia sobre
// RabbitMQ (INF-06). Expone una API única de publish/subscribe para que
// ningún servicio tenga que resolver por su cuenta el envelope de eventos,
// la topología de colas, el ack manual, los reintentos ni la reconexión.
package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// DefaultEventVersion es la versión con la que nace todo evento nuevo. Un
// evento existente nunca cambia de forma: si el payload tiene que cambiar de
// manera incompatible se publica la versión siguiente en paralelo y se
// deprecia la anterior.
const DefaultEventVersion = 1

// eventTypePattern implementa el naming `<servicio>.<agregado>.<evento-en-pasado>`
// de las convenciones del equipo (ej. `chat.message.sent`). Son exactamente
// tres segmentos en minúscula: los bindings de los consumers dependen de esa
// forma (`community.#`, `*.member.banned`), así que un evento con otra forma
// no le llega a quien debería y conviene rechazarlo al construirlo.
var eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*(?:\.[a-z][a-z0-9]*(?:-[a-z0-9]+)*){2}$`)

// Envelope es el sobre común obligatorio de todo evento del bus, definido en
// INF-02 (`discordia-docs/arquitectura/eventos.md`). Los campos van en
// camelCase porque el mismo formato viaja hasta el front.
type Envelope struct {
	// EventID identifica unívocamente esta publicación. Es la clave de
	// idempotencia: un consumer que ya procesó este EventID tiene que poder
	// descartarlo sin efectos.
	EventID string `json:"eventId"`

	// EventType es el nombre del evento y, a la vez, la routing key con la
	// que se publica en el exchange.
	EventType string `json:"eventType"`

	// EventVersion permite convivir dos formas del mismo evento durante una
	// migración.
	EventVersion int `json:"eventVersion"`

	// OccurredAt es cuándo ocurrió el hecho, en UTC. Es la única fuente de
	// verdad sobre qué es más nuevo: el bus no garantiza orden de llegada.
	OccurredAt time.Time `json:"occurredAt"`

	// CorrelationID es el hilo que une todos los eventos de una misma acción
	// del usuario, de punta a punta entre servicios.
	CorrelationID string `json:"correlationId"`

	// CausationID es el EventID del evento que provocó este. Vacío si el
	// evento nace de una acción directa y no de otro evento.
	CausationID string `json:"causationId,omitempty"`

	// Producer es el servicio que publicó el evento (`chat`, `identity`, …).
	Producer string `json:"producer"`

	// Data es el payload propio del evento. Se deja crudo para que cada
	// consumer lo deserialice en su propio tipo con UnmarshalData.
	Data json.RawMessage `json:"data"`
}

// EnvelopeOption ajusta un Envelope en construcción.
type EnvelopeOption func(*Envelope)

// WithCorrelationID fija el correlation ID en vez de generar uno nuevo. Es lo
// que hay que usar cuando la acción ya viene con uno desde el api-gateway.
func WithCorrelationID(id string) EnvelopeOption {
	return func(e *Envelope) { e.CorrelationID = id }
}

// WithCause marca este evento como consecuencia de otro: hereda su
// correlation ID y guarda su EventID como causation ID. Es la forma de que
// una cadena de reacciones asíncronas siga siendo trazable.
func WithCause(parent Envelope) EnvelopeOption {
	return func(e *Envelope) {
		e.CorrelationID = parent.CorrelationID
		e.CausationID = parent.EventID
	}
}

// WithEventVersion publica una versión distinta de la inicial del evento.
func WithEventVersion(version int) EnvelopeOption {
	return func(e *Envelope) { e.EventVersion = version }
}

// WithOccurredAt fija el momento del hecho cuando no es "ahora" — por
// ejemplo al republicar algo ya ocurrido, o en tests deterministas.
func WithOccurredAt(t time.Time) EnvelopeOption {
	return func(e *Envelope) { e.OccurredAt = t.UTC() }
}

// NewEnvelope arma un evento listo para publicar: genera EventID y
// correlation ID, sella OccurredAt en UTC, serializa data y valida el
// resultado. Devuelve error antes de tocar el bus si algo no cumple el
// contrato.
func NewEnvelope(eventType, producer string, data any, opts ...EnvelopeOption) (Envelope, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal event data: %w", err)
	}

	envelope := Envelope{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		EventVersion:  DefaultEventVersion,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: uuid.NewString(),
		Producer:      producer,
		Data:          payload,
	}

	for _, opt := range opts {
		opt(&envelope)
	}

	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}

	return envelope, nil
}

// ErrInvalidEnvelope encabeza todo error de validación del sobre, para que un
// consumer pueda distinguir "el mensaje está mal formado" de "falló el
// handler".
var ErrInvalidEnvelope = errors.New("invalid envelope")

// Validate verifica que el sobre cumpla el contrato de INF-02.
func (e Envelope) Validate() error {
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("%w: eventId must be a UUID, got %q", ErrInvalidEnvelope, e.EventID)
	}
	if !eventTypePattern.MatchString(e.EventType) {
		return fmt.Errorf("%w: eventType %q must be <service>.<aggregate>.<past-tense-event>", ErrInvalidEnvelope, e.EventType)
	}
	if e.EventVersion < 1 {
		return fmt.Errorf("%w: eventVersion must be >= 1, got %d", ErrInvalidEnvelope, e.EventVersion)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurredAt is required", ErrInvalidEnvelope)
	}
	if _, err := uuid.Parse(e.CorrelationID); err != nil {
		return fmt.Errorf("%w: correlationId must be a UUID, got %q", ErrInvalidEnvelope, e.CorrelationID)
	}
	if e.CausationID != "" {
		if _, err := uuid.Parse(e.CausationID); err != nil {
			return fmt.Errorf("%w: causationId must be a UUID when present, got %q", ErrInvalidEnvelope, e.CausationID)
		}
	}
	if e.Producer == "" {
		return fmt.Errorf("%w: producer is required", ErrInvalidEnvelope)
	}
	if !isJSONObject(e.Data) {
		return fmt.Errorf("%w: data must be a JSON object", ErrInvalidEnvelope)
	}
	return nil
}

// RoutingKey es la clave con la que el evento se publica en el topic
// exchange. Coincide con el EventType por decisión de ADR-0003: así el
// binding de cada consumer se lee igual que el nombre del evento.
func (e Envelope) RoutingKey() string { return e.EventType }

// UnmarshalData deserializa el payload del evento en v.
func (e Envelope) UnmarshalData(v any) error {
	if err := json.Unmarshal(e.Data, v); err != nil {
		return fmt.Errorf("unmarshal data of event %s: %w", e.EventType, err)
	}
	return nil
}

// isJSONObject exige que el payload sea un objeto y no un escalar ni null:
// un evento cuyo data es `5` o `null` no se puede extender después sin
// romper a sus consumers.
func isJSONObject(raw json.RawMessage) bool {
	var probe map[string]any
	return len(raw) > 0 && json.Unmarshal(raw, &probe) == nil && probe != nil
}
