package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPolicy() retryPolicy {
	return retryPolicy{maxRetries: 3, initial: 200 * time.Millisecond, max: 5 * time.Second}
}

func TestRetryPolicy_DelayGrowsExponentiallyWithinJitterBounds(t *testing.T) {
	p := testPolicy()

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{attempt: 1, expected: 200 * time.Millisecond},
		{attempt: 2, expected: 400 * time.Millisecond},
		{attempt: 3, expected: 800 * time.Millisecond},
		{attempt: 4, expected: 1600 * time.Millisecond},
	}

	for _, tc := range tests {
		// Se corre varias veces porque el jitter es aleatorio: lo que se
		// verifica es la cota, no un valor puntual.
		for i := 0; i < 50; i++ {
			delay := p.delay(tc.attempt)

			assert.GreaterOrEqual(t, delay, tc.expected/2, "intento %d", tc.attempt)
			assert.LessOrEqual(t, delay, tc.expected, "intento %d", tc.attempt)
		}
	}
}

func TestRetryPolicy_DelayIsCappedByMax(t *testing.T) {
	p := testPolicy()

	for _, attempt := range []int{10, 100, 1000} {
		delay := p.delay(attempt)

		assert.GreaterOrEqual(t, delay, p.max/2)
		assert.LessOrEqual(t, delay, p.max, "un backoff sin techo deja eventos parados horas")
	}
}

func TestRetryPolicy_DelayJitters(t *testing.T) {
	p := testPolicy()

	seen := map[time.Duration]struct{}{}
	for i := 0; i < 100; i++ {
		seen[p.delay(3)] = struct{}{}
	}

	assert.Greater(t, len(seen), 1,
		"sin jitter, N instancias que fallan por la misma causa reintentan todas en el mismo instante")
}

func TestRetryPolicy_DelayNormalizesInvalidAttempts(t *testing.T) {
	p := testPolicy()

	for _, attempt := range []int{0, -1} {
		delay := p.delay(attempt)

		assert.GreaterOrEqual(t, delay, p.initial/2)
		assert.LessOrEqual(t, delay, p.initial)
	}
}

func TestRetryPolicy_WaitSleepsBeforeRetrying(t *testing.T) {
	p := retryPolicy{maxRetries: 3, initial: 40 * time.Millisecond, max: time.Second}

	start := time.Now()
	err := p.wait(context.Background(), 1)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
}

func TestRetryPolicy_WaitStopsOnCancelledContext(t *testing.T) {
	p := retryPolicy{maxRetries: 3, initial: 10 * time.Second, max: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := p.wait(ctx, 1)

	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second, "un shutdown no espera a que termine el backoff")
}

func TestNewRetryPolicy_ReadsTheConfig(t *testing.T) {
	cfg := Config{MaxRetries: 5, RetryInitialDelay: time.Second, RetryMaxDelay: time.Minute}

	p := newRetryPolicy(cfg)

	assert.Equal(t, retryPolicy{maxRetries: 5, initial: time.Second, max: time.Minute}, p)
}
