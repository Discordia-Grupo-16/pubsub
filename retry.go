package pubsub

import (
	"context"
	"math/rand/v2"
	"time"
)

// retryPolicy resuelve cuánto esperar entre reintentos de un mismo evento.
// El backoff es exponencial con jitter: sin jitter, N instancias que fallan
// por la misma causa —una base caída, por ejemplo— reintentan todas en el
// mismo instante y le pegan al recurso justo cuando se está recuperando.
type retryPolicy struct {
	maxRetries int
	initial    time.Duration
	max        time.Duration
}

func newRetryPolicy(cfg Config) retryPolicy {
	return retryPolicy{
		maxRetries: cfg.MaxRetries,
		initial:    cfg.RetryInitialDelay,
		max:        cfg.RetryMaxDelay,
	}
}

// delay devuelve la espera antes del reintento número attempt (1 es el
// primer reintento). El valor cae entre la mitad y el total del backoff
// exponencial de ese intento, acotado por el máximo configurado.
func (p retryPolicy) delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	backoff := p.initial
	for i := 1; i < attempt && backoff < p.max; i++ {
		backoff *= 2
	}
	if backoff > p.max || backoff <= 0 {
		backoff = p.max
	}

	half := backoff / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// wait duerme lo que corresponda antes del reintento, o corta apenas se
// cancela el contexto: un shutdown no tiene por qué esperar a que termine el
// backoff de un evento.
func (p retryPolicy) wait(ctx context.Context, attempt int) error {
	timer := time.NewTimer(p.delay(attempt))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
