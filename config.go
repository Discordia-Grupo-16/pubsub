package pubsub

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Valores por defecto del cliente. Los de topología vienen de ADR-0003; los
// de reintentos y prefetch son el pendiente que ese mismo ADR delegó a
// INF-06 y quedan justificados en el README.
const (
	// DefaultExchange es el topic exchange durable por el que viajan todos
	// los eventos de dominio.
	DefaultExchange = "discordia.events"

	// DefaultPrefetch acota cuántos mensajes sin ackear le puede entregar el
	// broker a un consumer. Con 16, una instancia que se cae devuelve a la
	// cola a lo sumo 16 eventos para que otra los tome, y alcanza para que
	// el consumo no quede serializado por la latencia de cada handler.
	DefaultPrefetch = 16

	// DefaultMaxRetries son los reintentos en proceso ante un fallo
	// transitorio, antes de mandar el evento a la dead-letter queue.
	DefaultMaxRetries = 3

	// DefaultRetryInitialDelay y DefaultRetryMaxDelay acotan el backoff
	// exponencial entre reintentos.
	DefaultRetryInitialDelay = 200 * time.Millisecond
	DefaultRetryMaxDelay     = 5 * time.Second

	// DefaultPublishTimeout es cuánto se espera la confirmación del broker
	// antes de dar una publicación por fallida.
	DefaultPublishTimeout = 5 * time.Second

	// DefaultReconnectInitialDelay y DefaultReconnectMaxDelay acotan el
	// backoff de reconexión cuando se cae el broker.
	DefaultReconnectInitialDelay = 500 * time.Millisecond
	DefaultReconnectMaxDelay     = 30 * time.Second

	// deadLetterSuffix se le agrega al exchange principal para nombrar el
	// dead-letter exchange cuando no se configura uno explícito.
	deadLetterSuffix = ".dlx"
)

// defaultURL no lleva credenciales a propósito: `guest:guest` es el default
// del propio RabbitMQ para conexiones locales, y así el repo no tiene ni
// siquiera credenciales de mentira escritas en el código. En cualquier
// entorno que no sea local, RABBITMQ_URL las trae desde el sistema de
// secretos.
const defaultURL = "amqp://localhost:5672/"

// Config es la configuración del cliente de Pub/Sub. Todos los servicios
// leen las mismas variables de entorno, así que un cambio de política se
// aplica igual en los ocho.
type Config struct {
	// URL es la conexión AMQP al broker, credenciales incluidas.
	URL string

	// Exchange es el topic exchange durable de eventos de dominio.
	Exchange string

	// DeadLetterExchange recibe los eventos que agotaron sus reintentos o
	// que fallaron de forma permanente.
	DeadLetterExchange string

	// ServiceName identifica al servicio que usa el cliente. Nombra la
	// conexión en la management UI —lo que hace legible el flujo durante la
	// demo— y es el prefijo natural de sus colas.
	ServiceName string

	// Prefetch es el límite de mensajes sin ackear por consumer.
	Prefetch int

	// MaxRetries son los reintentos en proceso ante un fallo transitorio.
	// Cero significa "un intento y a la DLQ".
	MaxRetries int

	// RetryInitialDelay y RetryMaxDelay acotan el backoff exponencial entre
	// reintentos de un mismo evento.
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration

	// PublishTimeout limita la espera de la confirmación del broker.
	PublishTimeout time.Duration

	// ReconnectInitialDelay y ReconnectMaxDelay acotan el backoff de
	// reconexión al broker.
	ReconnectInitialDelay time.Duration
	ReconnectMaxDelay     time.Duration
}

// ErrInvalidConfig encabeza todo error de configuración.
var ErrInvalidConfig = errors.New("invalid pubsub config")

// LoadConfig lee la configuración del entorno del proceso y la valida.
//
// A diferencia de la config de un servicio, acá un valor mal escrito no cae
// al default en silencio: un PUBSUB_PREFETCH inválido que se ignora sin
// avisar se descubre en producción como un consumo raro, no como un error.
// El servicio decide qué hacer con el error; lo esperable es no arrancar.
func LoadConfig() (Config, error) {
	cfg := Config{
		URL:         getEnv("RABBITMQ_URL", defaultURL),
		Exchange:    getEnv("RABBITMQ_EXCHANGE", DefaultExchange),
		ServiceName: getEnv("SERVICE_NAME", ""),
	}
	cfg.DeadLetterExchange = getEnv("RABBITMQ_DEAD_LETTER_EXCHANGE", cfg.Exchange+deadLetterSuffix)

	var err error
	if cfg.Prefetch, err = getEnvInt("PUBSUB_PREFETCH", DefaultPrefetch); err != nil {
		return Config{}, err
	}
	if cfg.MaxRetries, err = getEnvInt("PUBSUB_MAX_RETRIES", DefaultMaxRetries); err != nil {
		return Config{}, err
	}
	if cfg.RetryInitialDelay, err = getEnvDuration("PUBSUB_RETRY_INITIAL_DELAY", DefaultRetryInitialDelay); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxDelay, err = getEnvDuration("PUBSUB_RETRY_MAX_DELAY", DefaultRetryMaxDelay); err != nil {
		return Config{}, err
	}
	if cfg.PublishTimeout, err = getEnvDuration("PUBSUB_PUBLISH_TIMEOUT", DefaultPublishTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ReconnectInitialDelay, err = getEnvDuration("PUBSUB_RECONNECT_INITIAL_DELAY", DefaultReconnectInitialDelay); err != nil {
		return Config{}, err
	}
	if cfg.ReconnectMaxDelay, err = getEnvDuration("PUBSUB_RECONNECT_MAX_DELAY", DefaultReconnectMaxDelay); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate verifica que la configuración sea utilizable.
func (c Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("%w: URL is required", ErrInvalidConfig)
	}
	if c.Exchange == "" {
		return fmt.Errorf("%w: Exchange is required", ErrInvalidConfig)
	}
	if c.DeadLetterExchange == "" {
		return fmt.Errorf("%w: DeadLetterExchange is required", ErrInvalidConfig)
	}
	if c.DeadLetterExchange == c.Exchange {
		return fmt.Errorf("%w: DeadLetterExchange must differ from Exchange, both are %q", ErrInvalidConfig, c.Exchange)
	}
	if c.ServiceName == "" {
		return fmt.Errorf("%w: ServiceName is required (set SERVICE_NAME)", ErrInvalidConfig)
	}
	if c.Prefetch < 1 {
		return fmt.Errorf("%w: Prefetch must be >= 1, got %d", ErrInvalidConfig, c.Prefetch)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("%w: MaxRetries must be >= 0, got %d", ErrInvalidConfig, c.MaxRetries)
	}
	if c.RetryInitialDelay <= 0 {
		return fmt.Errorf("%w: RetryInitialDelay must be > 0, got %s", ErrInvalidConfig, c.RetryInitialDelay)
	}
	if c.RetryMaxDelay < c.RetryInitialDelay {
		return fmt.Errorf("%w: RetryMaxDelay (%s) must be >= RetryInitialDelay (%s)", ErrInvalidConfig, c.RetryMaxDelay, c.RetryInitialDelay)
	}
	if c.PublishTimeout <= 0 {
		return fmt.Errorf("%w: PublishTimeout must be > 0, got %s", ErrInvalidConfig, c.PublishTimeout)
	}
	if c.ReconnectInitialDelay <= 0 {
		return fmt.Errorf("%w: ReconnectInitialDelay must be > 0, got %s", ErrInvalidConfig, c.ReconnectInitialDelay)
	}
	if c.ReconnectMaxDelay < c.ReconnectInitialDelay {
		return fmt.Errorf("%w: ReconnectMaxDelay (%s) must be >= ReconnectInitialDelay (%s)", ErrInvalidConfig, c.ReconnectMaxDelay, c.ReconnectInitialDelay)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%w: %s=%q is not an integer", ErrInvalidConfig, key, v)
	}
	return n, nil
}

func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%w: %s=%q is not a duration (use 200ms, 5s, 1m)", ErrInvalidConfig, key, v)
	}
	return d, nil
}
