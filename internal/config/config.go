// Package config carga la configuración de discordia-chat desde variables de entorno.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config agrupa la configuración del servicio. Todos los campos tienen un
// valor por defecto razonable para desarrollo local; en despliegue se
// sobreescriben por variable de entorno (ver .env.example).
type Config struct {
	Env      string
	Port     string
	MongoURI string
	MongoDB  string
	LogLevel string

	// MongoMinPoolSize y MongoMaxPoolSize acotan el pool de conexiones del
	// cliente de Mongo. Con MaxPoolSize bajo, un pico de tráfico hace cola
	// en vez de agotar el servidor de Mongo.
	MongoMinPoolSize uint64
	MongoMaxPoolSize uint64

	// MongoConnectTimeout limita cuánto se espera al establecer la conexión
	// TCP inicial. MongoServerSelectionTimeout limita cuánto se espera a que
	// el driver encuentre un servidor apto para una operación (incluye
	// reintentos de descubrimiento de topología). MongoOperationTimeout es
	// el timeout por defecto de cada operación (CSOT) cuando el caller no
	// da uno más corto por contexto.
	MongoConnectTimeout         time.Duration
	MongoServerSelectionTimeout time.Duration
	MongoOperationTimeout       time.Duration
}

// Load lee la configuración desde el entorno del proceso.
func Load() Config {
	return Config{
		Env:      getEnv("APP_ENV", "development"),
		Port:     getEnv("PORT", "8080"),
		MongoURI: getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDB:  getEnv("MONGO_DB_NAME", "discordia_chat"),
		LogLevel: getEnv("LOG_LEVEL", "info"),

		MongoMinPoolSize: getEnvUint64("MONGO_MIN_POOL_SIZE", 0),
		MongoMaxPoolSize: getEnvUint64("MONGO_MAX_POOL_SIZE", 100),

		MongoConnectTimeout:         getEnvDuration("MONGO_CONNECT_TIMEOUT", 5*time.Second),
		MongoServerSelectionTimeout: getEnvDuration("MONGO_SERVER_SELECTION_TIMEOUT", 5*time.Second),
		MongoOperationTimeout:       getEnvDuration("MONGO_OPERATION_TIMEOUT", 10*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvUint64(key string, fallback uint64) uint64 {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
