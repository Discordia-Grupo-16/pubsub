package pubsub_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Discordia-Grupo-16/pubsub"
)

// Los fixtures de testdata/ los emitió cada cliente y los leen los dos: son
// el contrato del sobre escrito en un archivo. Si alguno de los dos lados
// cambia la forma del JSON, este test y su equivalente en Python
// (python/tests/test_interop.py) se rompen en el mismo PR.
//
// El caso que más importa es occurredAt: Go serializa con precisión de
// nanosegundos y Python solo llega a microsegundos.

func TestInterop_ReadsTheEnvelopeEmittedByTheGoClient(t *testing.T) {
	event := readFixture(t, "testdata/event-from-go.json")

	require.NoError(t, event.Validate())
	assert.Equal(t, "chat.message.sent", event.EventType)
	assert.Equal(t, "chat", event.Producer)
	assert.Equal(t, "c3a9f1d2-5e6b-4a70-8f21-9d4e7b0c5a38", event.CausationID)
	assert.Equal(t, time.Date(2026, 9, 13, 20, 0, 0, 123456789, time.UTC), event.OccurredAt)

	var payload struct {
		MessageID string `json:"messageId"`
		Content   string `json:"content"`
	}
	require.NoError(t, event.UnmarshalData(&payload))
	assert.Equal(t, "hola", payload.Content)
}

func TestInterop_ReadsTheEnvelopeEmittedByThePythonClient(t *testing.T) {
	event := readFixture(t, "testdata/event-from-python.json")

	require.NoError(t, event.Validate())
	assert.Equal(t, "identity.user.registered", event.EventType)
	assert.Equal(t, "identity", event.Producer)
	assert.Equal(t, "8b2d0b7e-4f3a-4c1b-9a1e-2c5d7f0a3b64", event.CorrelationID)
	assert.Equal(t, time.Date(2026, 9, 13, 20, 0, 0, 123456000, time.UTC), event.OccurredAt,
		"Python emite microsegundos; Go los lee sin perder nada")

	var payload struct {
		UserID string `json:"userId"`
		Email  string `json:"email"`
	}
	require.NoError(t, event.UnmarshalData(&payload))
	assert.Equal(t, "demo@discordia.test", payload.Email)
}

// Si este test falla es porque el cliente Go cambió la forma del JSON: hay
// que regenerar el fixture y actualizar el otro lado en el mismo PR.
func TestInterop_TheGoClientStillEmitsTheFixtureShape(t *testing.T) {
	event := readFixture(t, "testdata/event-from-go.json")

	raw, err := json.Marshal(event)
	require.NoError(t, err)

	assert.JSONEq(t, string(readFile(t, "testdata/event-from-go.json")), string(raw))
}

func readFixture(t *testing.T, path string) pubsub.Envelope {
	t.Helper()

	var event pubsub.Envelope
	require.NoError(t, json.Unmarshal(readFile(t, path), &event))
	return event
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}
