package pubsub_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Discordia-Grupo-16/pubsub"
)

type messageSent struct {
	MessageID string `json:"messageId"`
	ChannelID string `json:"channelId"`
}

func TestNewEnvelope_FillsDefaults(t *testing.T) {
	before := time.Now().UTC()

	envelope, err := pubsub.NewEnvelope("chat.message.sent", "chat", messageSent{
		MessageID: uuid.NewString(),
		ChannelID: uuid.NewString(),
	})

	require.NoError(t, err)
	assert.NotEmpty(t, envelope.EventID)
	assert.NotEqual(t, envelope.EventID, envelope.CorrelationID, "cada uno tiene que ser un UUID propio")
	assert.Equal(t, pubsub.DefaultEventVersion, envelope.EventVersion)
	assert.Empty(t, envelope.CausationID, "un evento que no reacciona a otro no tiene causa")
	assert.Equal(t, time.UTC, envelope.OccurredAt.Location())
	assert.False(t, envelope.OccurredAt.Before(before))
}

func TestNewEnvelope_RoutingKeyIsTheEventType(t *testing.T) {
	envelope, err := pubsub.NewEnvelope("community.member.joined", "community", map[string]string{"userId": "u1"})

	require.NoError(t, err)
	assert.Equal(t, "community.member.joined", envelope.RoutingKey())
}

func TestNewEnvelope_WithCorrelationID(t *testing.T) {
	correlationID := uuid.NewString()

	envelope, err := pubsub.NewEnvelope("identity.user.registered", "identity",
		map[string]string{"userId": "u1"},
		pubsub.WithCorrelationID(correlationID),
	)

	require.NoError(t, err)
	assert.Equal(t, correlationID, envelope.CorrelationID)
}

func TestNewEnvelope_WithCauseKeepsTheTrace(t *testing.T) {
	parent, err := pubsub.NewEnvelope("mod.member.banned", "mod", map[string]string{"userId": "u1"})
	require.NoError(t, err)

	child, err := pubsub.NewEnvelope("chat.session.closed", "chat",
		map[string]string{"userId": "u1"},
		pubsub.WithCause(parent),
	)

	require.NoError(t, err)
	assert.Equal(t, parent.CorrelationID, child.CorrelationID, "la reacción viaja en el mismo hilo que su causa")
	assert.Equal(t, parent.EventID, child.CausationID)
	assert.NotEqual(t, parent.EventID, child.EventID)
}

func TestNewEnvelope_WithEventVersionAndOccurredAt(t *testing.T) {
	occurred := time.Date(2026, 9, 13, 20, 0, 0, 0, time.FixedZone("ART", -3*60*60))

	envelope, err := pubsub.NewEnvelope("metrics.rollup.computed", "metrics",
		map[string]int{"count": 3},
		pubsub.WithEventVersion(2),
		pubsub.WithOccurredAt(occurred),
	)

	require.NoError(t, err)
	assert.Equal(t, 2, envelope.EventVersion)
	assert.Equal(t, occurred.UTC(), envelope.OccurredAt)
	assert.Equal(t, time.UTC, envelope.OccurredAt.Location(), "occurredAt siempre se guarda en UTC")
}

func TestNewEnvelope_RejectsUnserializableData(t *testing.T) {
	_, err := pubsub.NewEnvelope("chat.message.sent", "chat", make(chan int))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal event data")
}

func TestNewEnvelope_RejectsInvalidInput(t *testing.T) {
	tests := map[string]struct {
		eventType string
		producer  string
		data      any
	}{
		"eventType con menos de tres segmentos": {eventType: "chat.sent", producer: "chat", data: map[string]string{}},
		"eventType con más de tres segmentos":   {eventType: "chat.message.reaction.added", producer: "chat", data: map[string]string{}},
		"eventType en mayúsculas":               {eventType: "Chat.Message.Sent", producer: "chat", data: map[string]string{}},
		"eventType vacío":                       {eventType: "", producer: "chat", data: map[string]string{}},
		"producer vacío":                        {eventType: "chat.message.sent", producer: "", data: map[string]string{}},
		"data escalar":                          {eventType: "chat.message.sent", producer: "chat", data: 5},
		"data nula":                             {eventType: "chat.message.sent", producer: "chat", data: nil},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := pubsub.NewEnvelope(tc.eventType, tc.producer, tc.data)

			require.Error(t, err)
			assert.ErrorIs(t, err, pubsub.ErrInvalidEnvelope)
		})
	}
}

func TestNewEnvelope_AcceptsHyphenatedSegments(t *testing.T) {
	_, err := pubsub.NewEnvelope("chat-and-real-time.voice-channel.joined", "chat-and-real-time", map[string]string{"userId": "u1"})

	assert.NoError(t, err, "los servicios con guion en el nombre también tienen que poder publicar")
}

func TestValidate_RejectsBrokenIdentifiers(t *testing.T) {
	valid := func() pubsub.Envelope {
		envelope, err := pubsub.NewEnvelope("chat.message.sent", "chat", map[string]string{"messageId": "m1"})
		require.NoError(t, err)
		return envelope
	}

	tests := map[string]func(*pubsub.Envelope){
		"eventId no es UUID":       func(e *pubsub.Envelope) { e.EventID = "1" },
		"correlationId no es UUID": func(e *pubsub.Envelope) { e.CorrelationID = "no-uuid" },
		"causationId no es UUID":   func(e *pubsub.Envelope) { e.CausationID = "no-uuid" },
		"eventVersion en cero":     func(e *pubsub.Envelope) { e.EventVersion = 0 },
		"occurredAt sin setear":    func(e *pubsub.Envelope) { e.OccurredAt = time.Time{} },
		"data vacía":               func(e *pubsub.Envelope) { e.Data = nil },
	}

	for name, breakIt := range tests {
		t.Run(name, func(t *testing.T) {
			envelope := valid()
			breakIt(&envelope)

			err := envelope.Validate()

			require.Error(t, err)
			assert.ErrorIs(t, err, pubsub.ErrInvalidEnvelope)
		})
	}
}

func TestEnvelope_JSONUsesCamelCaseAndUTC(t *testing.T) {
	envelope, err := pubsub.NewEnvelope("chat.message.sent", "chat",
		messageSent{MessageID: "m1", ChannelID: "c1"},
		pubsub.WithOccurredAt(time.Date(2026, 9, 13, 20, 0, 0, 0, time.UTC)),
	)
	require.NoError(t, err)

	raw, err := json.Marshal(envelope)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))

	assert.ElementsMatch(t,
		[]string{"eventId", "eventType", "eventVersion", "occurredAt", "correlationId", "producer", "data"},
		keysOf(decoded),
		"causationId se omite cuando está vacío; el resto del sobre es obligatorio")
	assert.Equal(t, "2026-09-13T20:00:00Z", decoded["occurredAt"], "fechas en ISO-8601 UTC con Z")
}

func TestEnvelope_RoundTripPreservesData(t *testing.T) {
	original, err := pubsub.NewEnvelope("chat.message.sent", "chat", messageSent{MessageID: "m1", ChannelID: "c1"})
	require.NoError(t, err)

	raw, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded pubsub.Envelope
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.NoError(t, decoded.Validate())

	var payload messageSent
	require.NoError(t, decoded.UnmarshalData(&payload))

	assert.Equal(t, original.EventID, decoded.EventID)
	assert.Equal(t, original.OccurredAt, decoded.OccurredAt)
	assert.Equal(t, messageSent{MessageID: "m1", ChannelID: "c1"}, payload)
}

func TestEnvelope_UnmarshalDataFailsOnTypeMismatch(t *testing.T) {
	envelope, err := pubsub.NewEnvelope("chat.message.sent", "chat", map[string]string{"messageId": "m1"})
	require.NoError(t, err)

	var payload struct {
		MessageID int `json:"messageId"`
	}
	err = envelope.UnmarshalData(&payload)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "chat.message.sent", "el error dice de qué evento es el payload que no encaja")
}

func keysOf(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
