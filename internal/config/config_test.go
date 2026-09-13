package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLoad_Defaults(t *testing.T) {
	cfg := Load()

	assert.Equal(t, "development", cfg.Env)
	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, "mongodb://localhost:27017", cfg.MongoURI)
	assert.Equal(t, "discordia_chat", cfg.MongoDB)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, uint64(0), cfg.MongoMinPoolSize)
	assert.Equal(t, uint64(100), cfg.MongoMaxPoolSize)
	assert.Equal(t, 5*time.Second, cfg.MongoConnectTimeout)
	assert.Equal(t, 5*time.Second, cfg.MongoServerSelectionTimeout)
	assert.Equal(t, 10*time.Second, cfg.MongoOperationTimeout)
}

func TestLoad_OverridesFromEnv(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("PORT", "9090")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("MONGO_DB_NAME", "discordia_chat_prod")
	t.Setenv("LOG_LEVEL", "warn")
	t.Setenv("MONGO_MIN_POOL_SIZE", "5")
	t.Setenv("MONGO_MAX_POOL_SIZE", "50")
	t.Setenv("MONGO_CONNECT_TIMEOUT", "2s")
	t.Setenv("MONGO_SERVER_SELECTION_TIMEOUT", "3s")
	t.Setenv("MONGO_OPERATION_TIMEOUT", "15s")

	cfg := Load()

	assert.Equal(t, "production", cfg.Env)
	assert.Equal(t, "9090", cfg.Port)
	assert.Equal(t, "mongodb://mongo:27017", cfg.MongoURI)
	assert.Equal(t, "discordia_chat_prod", cfg.MongoDB)
	assert.Equal(t, "warn", cfg.LogLevel)
	assert.Equal(t, uint64(5), cfg.MongoMinPoolSize)
	assert.Equal(t, uint64(50), cfg.MongoMaxPoolSize)
	assert.Equal(t, 2*time.Second, cfg.MongoConnectTimeout)
	assert.Equal(t, 3*time.Second, cfg.MongoServerSelectionTimeout)
	assert.Equal(t, 15*time.Second, cfg.MongoOperationTimeout)
}

func TestLoad_EmptyEnvFallsBackToDefault(t *testing.T) {
	t.Setenv("PORT", "")

	cfg := Load()

	assert.Equal(t, "8080", cfg.Port)
}

func TestLoad_InvalidNumericEnvFallsBackToDefault(t *testing.T) {
	t.Setenv("MONGO_MAX_POOL_SIZE", "not-a-number")
	t.Setenv("MONGO_CONNECT_TIMEOUT", "not-a-duration")

	cfg := Load()

	assert.Equal(t, uint64(100), cfg.MongoMaxPoolSize)
	assert.Equal(t, 5*time.Second, cfg.MongoConnectTimeout)
}
