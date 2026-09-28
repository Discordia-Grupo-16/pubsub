package pubsub_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Discordia-Grupo-16/pubsub"
)

// clearPubsubEnv vacía todas las variables del cliente para que el test no
// dependa de lo que tenga exportado la shell de quien lo corre.
func clearPubsubEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"RABBITMQ_URL", "RABBITMQ_EXCHANGE", "RABBITMQ_DEAD_LETTER_EXCHANGE", "SERVICE_NAME",
		"PUBSUB_PREFETCH", "PUBSUB_MAX_RETRIES", "PUBSUB_RETRY_INITIAL_DELAY", "PUBSUB_RETRY_MAX_DELAY",
		"PUBSUB_PUBLISH_TIMEOUT", "PUBSUB_RECONNECT_INITIAL_DELAY", "PUBSUB_RECONNECT_MAX_DELAY",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	clearPubsubEnv(t)
	t.Setenv("SERVICE_NAME", "chat")

	cfg, err := pubsub.LoadConfig()

	require.NoError(t, err)
	assert.Equal(t, "amqp://localhost:5672/", cfg.URL)
	assert.Equal(t, "discordia.events", cfg.Exchange)
	assert.Equal(t, "discordia.events.dlx", cfg.DeadLetterExchange)
	assert.Equal(t, "chat", cfg.ServiceName)
	assert.Equal(t, 16, cfg.Prefetch)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.Equal(t, 200*time.Millisecond, cfg.RetryInitialDelay)
	assert.Equal(t, 5*time.Second, cfg.RetryMaxDelay)
	assert.Equal(t, 5*time.Second, cfg.PublishTimeout)
	assert.Equal(t, 500*time.Millisecond, cfg.ReconnectInitialDelay)
	assert.Equal(t, 30*time.Second, cfg.ReconnectMaxDelay)
}

func TestLoadConfig_OverridesFromEnv(t *testing.T) {
	clearPubsubEnv(t)
	t.Setenv("RABBITMQ_URL", "amqp://user:pass@rabbitmq:5672/discordia")
	t.Setenv("RABBITMQ_EXCHANGE", "discordia.events.test")
	t.Setenv("RABBITMQ_DEAD_LETTER_EXCHANGE", "discordia.dead")
	t.Setenv("SERVICE_NAME", "community")
	t.Setenv("PUBSUB_PREFETCH", "32")
	t.Setenv("PUBSUB_MAX_RETRIES", "5")
	t.Setenv("PUBSUB_RETRY_INITIAL_DELAY", "1s")
	t.Setenv("PUBSUB_RETRY_MAX_DELAY", "10s")
	t.Setenv("PUBSUB_PUBLISH_TIMEOUT", "2s")
	t.Setenv("PUBSUB_RECONNECT_INITIAL_DELAY", "100ms")
	t.Setenv("PUBSUB_RECONNECT_MAX_DELAY", "1m")

	cfg, err := pubsub.LoadConfig()

	require.NoError(t, err)
	assert.Equal(t, "amqp://user:pass@rabbitmq:5672/discordia", cfg.URL)
	assert.Equal(t, "discordia.events.test", cfg.Exchange)
	assert.Equal(t, "discordia.dead", cfg.DeadLetterExchange)
	assert.Equal(t, "community", cfg.ServiceName)
	assert.Equal(t, 32, cfg.Prefetch)
	assert.Equal(t, 5, cfg.MaxRetries)
	assert.Equal(t, time.Second, cfg.RetryInitialDelay)
	assert.Equal(t, 10*time.Second, cfg.RetryMaxDelay)
	assert.Equal(t, 2*time.Second, cfg.PublishTimeout)
	assert.Equal(t, 100*time.Millisecond, cfg.ReconnectInitialDelay)
	assert.Equal(t, time.Minute, cfg.ReconnectMaxDelay)
}

func TestLoadConfig_DeadLetterExchangeFollowsTheExchange(t *testing.T) {
	clearPubsubEnv(t)
	t.Setenv("SERVICE_NAME", "mod")
	t.Setenv("RABBITMQ_EXCHANGE", "discordia.events.ci")

	cfg, err := pubsub.LoadConfig()

	require.NoError(t, err)
	assert.Equal(t, "discordia.events.ci.dlx", cfg.DeadLetterExchange,
		"apuntar un exchange de prueba a la DLX de producción mezclaría eventos muertos de dos entornos")
}

func TestLoadConfig_EmptyValueFallsBackToDefault(t *testing.T) {
	clearPubsubEnv(t)
	t.Setenv("SERVICE_NAME", "chat")
	t.Setenv("PUBSUB_PREFETCH", "")

	cfg, err := pubsub.LoadConfig()

	require.NoError(t, err)
	assert.Equal(t, pubsub.DefaultPrefetch, cfg.Prefetch)
}

func TestLoadConfig_FailsOnMalformedValues(t *testing.T) {
	tests := map[string]struct{ key, value string }{
		"prefetch no numérico":        {"PUBSUB_PREFETCH", "muchos"},
		"reintentos no numérico":      {"PUBSUB_MAX_RETRIES", "3.5"},
		"delay inicial sin unidad":    {"PUBSUB_RETRY_INITIAL_DELAY", "200"},
		"delay máximo inválido":       {"PUBSUB_RETRY_MAX_DELAY", "un rato"},
		"timeout inválido":            {"PUBSUB_PUBLISH_TIMEOUT", "x"},
		"reconexión inicial inválida": {"PUBSUB_RECONNECT_INITIAL_DELAY", "x"},
		"reconexión máxima inválida":  {"PUBSUB_RECONNECT_MAX_DELAY", "x"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clearPubsubEnv(t)
			t.Setenv("SERVICE_NAME", "chat")
			t.Setenv(tc.key, tc.value)

			_, err := pubsub.LoadConfig()

			require.Error(t, err, "un valor mal escrito no puede caer al default en silencio")
			assert.ErrorIs(t, err, pubsub.ErrInvalidConfig)
			assert.Contains(t, err.Error(), tc.key)
		})
	}
}

func TestLoadConfig_RequiresServiceName(t *testing.T) {
	clearPubsubEnv(t)
	t.Setenv("SERVICE_NAME", "")

	_, err := pubsub.LoadConfig()

	require.Error(t, err)
	assert.ErrorIs(t, err, pubsub.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "SERVICE_NAME")
}

func TestConfig_ValidateRejectsInconsistentValues(t *testing.T) {
	valid := pubsub.Config{
		URL:                   "amqp://localhost:5672/",
		Exchange:              "discordia.events",
		DeadLetterExchange:    "discordia.events.dlx",
		ServiceName:           "chat",
		Prefetch:              16,
		MaxRetries:            3,
		RetryInitialDelay:     200 * time.Millisecond,
		RetryMaxDelay:         5 * time.Second,
		PublishTimeout:        5 * time.Second,
		ReconnectInitialDelay: 500 * time.Millisecond,
		ReconnectMaxDelay:     30 * time.Second,
	}
	require.NoError(t, valid.Validate())

	tests := map[string]func(*pubsub.Config){
		"sin URL":                              func(c *pubsub.Config) { c.URL = "" },
		"sin exchange":                         func(c *pubsub.Config) { c.Exchange = "" },
		"sin dead-letter exchange":             func(c *pubsub.Config) { c.DeadLetterExchange = "" },
		"DLX igual al exchange":                func(c *pubsub.Config) { c.DeadLetterExchange = c.Exchange },
		"sin nombre de servicio":               func(c *pubsub.Config) { c.ServiceName = "" },
		"prefetch en cero":                     func(c *pubsub.Config) { c.Prefetch = 0 },
		"reintentos negativos":                 func(c *pubsub.Config) { c.MaxRetries = -1 },
		"delay inicial en cero":                func(c *pubsub.Config) { c.RetryInitialDelay = 0 },
		"delay máximo menor al inicial":        func(c *pubsub.Config) { c.RetryMaxDelay = time.Millisecond },
		"timeout de publicación en cero":       func(c *pubsub.Config) { c.PublishTimeout = 0 },
		"reconexión inicial en cero":           func(c *pubsub.Config) { c.ReconnectInitialDelay = 0 },
		"reconexión máxima menor a la inicial": func(c *pubsub.Config) { c.ReconnectMaxDelay = time.Millisecond },
	}

	for name, breakIt := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			breakIt(&cfg)

			err := cfg.Validate()

			require.Error(t, err)
			assert.ErrorIs(t, err, pubsub.ErrInvalidConfig)
		})
	}
}

func TestConfig_MaxRetriesZeroIsValid(t *testing.T) {
	clearPubsubEnv(t)
	t.Setenv("SERVICE_NAME", "metrics")
	t.Setenv("PUBSUB_MAX_RETRIES", "0")

	cfg, err := pubsub.LoadConfig()

	require.NoError(t, err, "cero reintentos es una política válida: un intento y a la DLQ")
	assert.Equal(t, 0, cfg.MaxRetries)
}
