package pubsub

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errTransient = errors.New("connection reset by peer")
	errBusiness  = errors.New("channel does not belong to the server")
)

func fastPolicy(maxRetries int) retryPolicy {
	return retryPolicy{maxRetries: maxRetries, initial: time.Millisecond, max: 2 * time.Millisecond}
}

func testEvent(t *testing.T) Envelope {
	t.Helper()
	event, err := NewEnvelope("chat.message.sent", "chat", map[string]string{"messageId": "m1"})
	require.NoError(t, err)
	return event
}

// countingHandler falla las primeras failures veces y después funciona.
func countingHandler(failures int, err error) (Handler, *int) {
	calls := 0
	return func(context.Context, Envelope) error {
		calls++
		if calls <= failures {
			return err
		}
		return nil
	}, &calls
}

func TestRunWithRetries_SucceedsOnFirstTry(t *testing.T) {
	handler, calls := countingHandler(0, errTransient)

	err := runWithRetries(context.Background(), fastPolicy(3), handler, testEvent(t))

	require.NoError(t, err)
	assert.Equal(t, 1, *calls, "un handler que anda no se reintenta")
}

func TestRunWithRetries_RetriesTransientFailures(t *testing.T) {
	handler, calls := countingHandler(2, errTransient)

	err := runWithRetries(context.Background(), fastPolicy(3), handler, testEvent(t))

	require.NoError(t, err)
	assert.Equal(t, 3, *calls, "dos fallos transitorios y a la tercera sale")
}

func TestRunWithRetries_GivesUpAfterMaxRetries(t *testing.T) {
	handler, calls := countingHandler(99, errTransient)

	err := runWithRetries(context.Background(), fastPolicy(3), handler, testEvent(t))

	require.ErrorIs(t, err, errTransient)
	assert.Equal(t, 4, *calls, "el intento original más tres reintentos")
}

func TestRunWithRetries_DoesNotRetryPermanentFailures(t *testing.T) {
	calls := 0
	handler := func(context.Context, Envelope) error {
		calls++
		return Permanent(errBusiness)
	}

	err := runWithRetries(context.Background(), fastPolicy(3), handler, testEvent(t))

	require.True(t, IsPermanent(err))
	assert.Equal(t, 1, calls, "un fallo permanente va a fallar igual las tres veces siguientes")
}

func TestRunWithRetries_StopsRetryingWhenTheFailureBecomesPermanent(t *testing.T) {
	calls := 0
	handler := func(context.Context, Envelope) error {
		calls++
		if calls == 1 {
			return errTransient
		}
		return Permanent(errBusiness)
	}

	err := runWithRetries(context.Background(), fastPolicy(5), handler, testEvent(t))

	require.True(t, IsPermanent(err))
	assert.Equal(t, 2, calls)
}

func TestRunWithRetries_WithZeroRetries(t *testing.T) {
	handler, calls := countingHandler(99, errTransient)

	err := runWithRetries(context.Background(), fastPolicy(0), handler, testEvent(t))

	require.ErrorIs(t, err, errTransient)
	assert.Equal(t, 1, *calls, "cero reintentos es un intento y a la DLQ")
}

func TestRunWithRetries_StopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	handler := func(context.Context, Envelope) error {
		calls++
		cancel()
		return errTransient
	}

	err := runWithRetries(ctx, retryPolicy{maxRetries: 5, initial: time.Second, max: time.Minute}, handler, testEvent(t))

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls, "un shutdown no espera a que se agoten los reintentos")
}

func TestRunWithRetries_PassesTheEventThrough(t *testing.T) {
	event := testEvent(t)
	var received Envelope

	err := runWithRetries(context.Background(), fastPolicy(0), func(_ context.Context, e Envelope) error {
		received = e
		return nil
	}, event)

	require.NoError(t, err)
	assert.Equal(t, event, received)
}

func TestDecideAck(t *testing.T) {
	tests := map[string]struct {
		err      error
		expected ackDecision
	}{
		"éxito":                     {nil, ackEvent},
		"fallo transitorio agotado": {errTransient, deadLetterEvent},
		"fallo permanente":          {Permanent(errBusiness), deadLetterEvent},
		"contexto cancelado":        {context.Canceled, requeueEvent},
		"contexto vencido":          {context.DeadlineExceeded, requeueEvent},
		"cancelación envuelta":      {errors.Join(errTransient, context.Canceled), requeueEvent},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, decideAck(tc.err))
		})
	}
}

func TestSubscribe_RejectsInvalidInput(t *testing.T) {
	client := &Client{cfg: testConfig(), logger: slog.New(discardHandler()), shutdown: make(chan struct{})}
	valid := Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.message.sent"}}

	t.Run("subscripción inválida", func(t *testing.T) {
		err := client.Subscribe(context.Background(), Subscription{BindingKeys: []string{"chat.#"}}, func(context.Context, Envelope) error { return nil })

		assert.ErrorIs(t, err, ErrInvalidSubscription)
	})

	t.Run("sin handler", func(t *testing.T) {
		err := client.Subscribe(context.Background(), valid, nil)

		assert.ErrorIs(t, err, ErrInvalidSubscription)
	})

	t.Run("sin conexión", func(t *testing.T) {
		err := client.Subscribe(context.Background(), valid, func(context.Context, Envelope) error { return nil })

		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestSubscribe_OnClosedClient(t *testing.T) {
	client := &Client{cfg: testConfig(), logger: slog.New(discardHandler()), shutdown: make(chan struct{})}
	require.NoError(t, client.Close())

	err := client.Subscribe(context.Background(), Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.#"}},
		func(context.Context, Envelope) error { return nil })

	assert.ErrorIs(t, err, ErrClosed)
}

func TestRestartSubscriptions_SkipsCancelledOnes(t *testing.T) {
	client := &Client{cfg: testConfig(), logger: slog.New(discardHandler()), shutdown: make(chan struct{})}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	client.subs = []*subscription{{
		spec:    Subscription{Queue: PerInstanceQueue(), BindingKeys: []string{"chat.#"}},
		handler: func(context.Context, Envelope) error { return nil },
		ctx:     cancelled,
	}}

	// Sin conexión, rehacer una subscripción viva fallaría; que no falle
	// prueba que la cancelada se salteó.
	assert.NoError(t, client.restartSubscriptions())
}

func TestSubscriptionContext_CancelsOnClientShutdown(t *testing.T) {
	client := &Client{cfg: testConfig(), logger: slog.New(discardHandler()), shutdown: make(chan struct{})}
	ctx, cancel := client.subscriptionContext(context.Background())
	defer cancel()

	close(client.shutdown)

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("cerrar el cliente tiene que cortar el backoff de un evento en vuelo")
	}
}

func TestSubscriptionContext_CancelsWithItsParent(t *testing.T) {
	client := &Client{cfg: testConfig(), logger: slog.New(discardHandler()), shutdown: make(chan struct{})}
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := client.subscriptionContext(parent)
	defer cancel()

	cancelParent()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("cancelar el contexto de la subscripción tiene que frenarla")
	}
}
