package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect_RejectsInvalidConfig(t *testing.T) {
	_, err := Connect(context.Background(), Config{})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidConfig, "no se intenta conectar con una config que no cierra")
}

func TestConnect_FailsFastOnUnreachableBroker(t *testing.T) {
	cfg := testConfig()
	cfg.URL = "amqp://127.0.0.1:1/"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := Connect(ctx, cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "dial broker",
		"un servicio que arranca sin bus falla en silencio en la primera operación; mejor no arrancar")
}

func TestWithLogger(t *testing.T) {
	logger := slog.New(discardHandler())
	client := &Client{logger: slog.Default()}

	WithLogger(logger)(client)
	assert.Same(t, logger, client.logger)

	WithLogger(nil)(client)
	assert.Same(t, logger, client.logger, "un logger nil no puede dejar al cliente sin logger")
}

func TestPublish_ValidatesBeforeTouchingTheBroker(t *testing.T) {
	client := &Client{cfg: testConfig(), logger: slog.New(discardHandler()), shutdown: make(chan struct{})}

	err := client.Publish(context.Background(), Envelope{EventType: "chat.message.sent"})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidEnvelope)
}

func TestPublish_OnClosedClient(t *testing.T) {
	client := &Client{cfg: testConfig(), logger: slog.New(discardHandler()), shutdown: make(chan struct{})}
	event, err := NewEnvelope("chat.message.sent", "chat", map[string]string{"messageId": "m1"})
	require.NoError(t, err)

	err = client.Publish(context.Background(), event)

	assert.ErrorIs(t, err, ErrClosed)
}

func TestClose_IsIdempotent(t *testing.T) {
	client := &Client{cfg: testConfig(), logger: slog.New(discardHandler()), shutdown: make(chan struct{})}

	require.NoError(t, client.Close())
	require.NoError(t, client.Close(), "cerrar dos veces no puede explotar: pasa en cualquier defer")
}

func TestPublishing_CarriesTheEnvelopeIdentifiersInTheProperties(t *testing.T) {
	occurred := time.Date(2026, 9, 13, 20, 0, 0, 0, time.UTC)
	event, err := NewEnvelope("chat.message.sent", "chat",
		map[string]string{"messageId": "m1"},
		WithOccurredAt(occurred),
	)
	require.NoError(t, err)
	body, err := json.Marshal(event)
	require.NoError(t, err)

	message := publishing(event, body)

	assert.Equal(t, event.EventID, message.MessageId)
	assert.Equal(t, event.CorrelationID, message.CorrelationId)
	assert.Equal(t, "chat.message.sent", message.Type)
	assert.Equal(t, "chat", message.AppId)
	assert.Equal(t, occurred, message.Timestamp)
	assert.Equal(t, "application/json", message.ContentType)
	assert.Equal(t, uint8(amqp.Persistent), message.DeliveryMode)
	assert.Equal(t, body, message.Body)
}

// discardHandler evita ensuciar la salida de los tests. No se usa
// slog.DiscardHandler para no exigir Go 1.24 a los servicios que consumen
// esta librería.
func discardHandler() slog.Handler {
	return slog.NewTextHandler(io.Discard, nil)
}
