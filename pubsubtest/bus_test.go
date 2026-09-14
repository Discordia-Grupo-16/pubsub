package pubsubtest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Discordia-Grupo-16/pubsub"
	"github.com/Discordia-Grupo-16/pubsub/pubsubtest"
)

var errHandler = errors.New("projection is down")

func event(t *testing.T, eventType string) pubsub.Envelope {
	t.Helper()
	envelope, err := pubsub.NewEnvelope(eventType, "community", map[string]string{"serverId": "s1"})
	require.NoError(t, err)
	return envelope
}

func collector(received *[]pubsub.Envelope) pubsub.Handler {
	return func(_ context.Context, e pubsub.Envelope) error {
		*received = append(*received, e)
		return nil
	}
}

func TestBus_DeliversSynchronously(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()
	var received []pubsub.Envelope
	require.NoError(t, bus.Subscribe(ctx, pubsub.Subscription{
		Queue:       pubsub.SharedQueue("chat.projection"),
		BindingKeys: []string{"community.member.joined"},
	}, collector(&received)))

	sent := event(t, "community.member.joined")
	require.NoError(t, bus.Publish(ctx, sent))

	require.Len(t, received, 1, "cuando Publish vuelve, el handler ya corrió: el test no necesita esperas")
	assert.Equal(t, sent.EventID, received[0].EventID)
}

func TestBus_RoutesByBindingKey(t *testing.T) {
	tests := map[string]struct {
		bindingKey string
		eventType  string
		delivered  bool
	}{
		"exacta":                          {"community.member.joined", "community.member.joined", true},
		"exacta que no coincide":          {"community.member.joined", "community.member.left", false},
		"comodín de un segmento":          {"community.member.*", "community.member.left", true},
		"un segmento no cubre dos":        {"community.*", "community.member.left", false},
		"comodín de varios segmentos":     {"community.#", "community.member.left", true},
		"varios segmentos cubre el resto": {"community.server.#", "community.server.created", true},
		"todo el bus":                     {"#", "chat.message.sent", true},
		"comodín en el medio":             {"community.#.joined", "community.member.joined", true},
		"otro servicio":                   {"community.#", "chat.message.sent", false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			bus := pubsubtest.New()
			var received []pubsub.Envelope
			require.NoError(t, bus.Subscribe(ctx, pubsub.Subscription{
				Queue:       pubsub.PerInstanceQueue(),
				BindingKeys: []string{tc.bindingKey},
			}, collector(&received)))

			require.NoError(t, bus.Publish(ctx, event(t, tc.eventType)))

			assert.Equal(t, tc.delivered, len(received) == 1)
		})
	}
}

// Estas dos propiedades son la razón de ser del fake: reproducen la
// diferencia entre las dos semánticas de cola sin necesidad de un broker.
func TestBus_SharedQueueSplitsEventsBetweenReplicas(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()
	var first, second []pubsub.Envelope
	spec := pubsub.Subscription{Queue: pubsub.SharedQueue("chat.community-projection"), BindingKeys: []string{"community.#"}}
	require.NoError(t, bus.Subscribe(ctx, spec, collector(&first)))
	require.NoError(t, bus.Subscribe(ctx, spec, collector(&second)))

	for i := 0; i < 4; i++ {
		require.NoError(t, bus.Publish(ctx, event(t, "community.member.joined")))
	}

	assert.Len(t, first, 2)
	assert.Len(t, second, 2, "las réplicas compiten por la cola: cada evento lo procesa una sola")
}

func TestBus_PerInstanceQueueFansOutToEveryInstance(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()
	var first, second []pubsub.Envelope
	spec := pubsub.Subscription{Queue: pubsub.PerInstanceQueue(), BindingKeys: []string{"chat.message.sent"}}
	require.NoError(t, bus.Subscribe(ctx, spec, collector(&first)))
	require.NoError(t, bus.Subscribe(ctx, spec, collector(&second)))

	require.NoError(t, bus.Publish(ctx, event(t, "chat.message.sent")))

	assert.Len(t, first, 1)
	assert.Len(t, second, 1, "es el fan-out que el CP1 tiene que demostrar: le llega a las dos instancias")
}

func TestBus_DifferentSharedQueuesEachGetTheEvent(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()
	var chat, metrics []pubsub.Envelope
	require.NoError(t, bus.Subscribe(ctx, pubsub.Subscription{
		Queue: pubsub.SharedQueue("chat.projection"), BindingKeys: []string{"community.#"},
	}, collector(&chat)))
	require.NoError(t, bus.Subscribe(ctx, pubsub.Subscription{
		Queue: pubsub.SharedQueue("metrics.all"), BindingKeys: []string{"#"},
	}, collector(&metrics)))

	require.NoError(t, bus.Publish(ctx, event(t, "community.member.joined")))

	assert.Len(t, chat, 1)
	assert.Len(t, metrics, 1, "dos servicios distintos reciben el mismo evento cada uno en su cola")
}

func TestBus_SkipsCancelledSubscriptions(t *testing.T) {
	bus := pubsubtest.New()
	cancelled, cancel := context.WithCancel(context.Background())
	var received []pubsub.Envelope
	require.NoError(t, bus.Subscribe(cancelled, pubsub.Subscription{
		Queue: pubsub.PerInstanceQueue(), BindingKeys: []string{"#"},
	}, collector(&received)))
	cancel()

	require.NoError(t, bus.Publish(context.Background(), event(t, "community.member.joined")))

	assert.Empty(t, received)
}

func TestBus_RecordsWhatWasPublished(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()

	require.NoError(t, bus.Publish(ctx, event(t, "community.server.created")))
	require.NoError(t, bus.Publish(ctx, event(t, "community.member.joined")))

	assert.Len(t, bus.Published(), 2)
	require.Len(t, bus.PublishedOf("community.member.joined"), 1)
	assert.Empty(t, bus.PublishedOf("chat.message.sent"))
}

func TestBus_RecordsDeadLetteredEvents(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()
	require.NoError(t, bus.Subscribe(ctx, pubsub.Subscription{
		Queue: pubsub.SharedQueue("chat.projection"), BindingKeys: []string{"#"},
	}, func(context.Context, pubsub.Envelope) error { return errHandler }))

	require.NoError(t, bus.Publish(ctx, event(t, "community.member.joined")))

	assert.Len(t, bus.DeadLettered(), 1, "un handler que falla deja rastro: el test que espera que todo ande lo ve")
}

func TestBus_RetriesTransientFailures(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()
	bus.MaxRetries = 2
	calls := 0
	require.NoError(t, bus.Subscribe(ctx, pubsub.Subscription{
		Queue: pubsub.SharedQueue("chat.projection"), BindingKeys: []string{"#"},
	}, func(context.Context, pubsub.Envelope) error {
		calls++
		if calls < 3 {
			return errHandler
		}
		return nil
	}))

	require.NoError(t, bus.Publish(ctx, event(t, "community.member.joined")))

	assert.Equal(t, 3, calls)
	assert.Empty(t, bus.DeadLettered())
}

func TestBus_DoesNotRetryPermanentFailures(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()
	bus.MaxRetries = 5
	calls := 0
	require.NoError(t, bus.Subscribe(ctx, pubsub.Subscription{
		Queue: pubsub.SharedQueue("chat.projection"), BindingKeys: []string{"#"},
	}, func(context.Context, pubsub.Envelope) error {
		calls++
		return pubsub.Permanent(errHandler)
	}))

	require.NoError(t, bus.Publish(ctx, event(t, "community.member.joined")))

	assert.Equal(t, 1, calls)
	assert.Len(t, bus.DeadLettered(), 1)
}

func TestBus_ValidatesLikeTheRealClient(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()

	t.Run("subscripción sin semántica de cola", func(t *testing.T) {
		err := bus.Subscribe(ctx, pubsub.Subscription{BindingKeys: []string{"#"}},
			func(context.Context, pubsub.Envelope) error { return nil })

		assert.ErrorIs(t, err, pubsub.ErrInvalidSubscription)
	})

	t.Run("subscripción sin handler", func(t *testing.T) {
		err := bus.Subscribe(ctx, pubsub.Subscription{Queue: pubsub.PerInstanceQueue(), BindingKeys: []string{"#"}}, nil)

		assert.ErrorIs(t, err, pubsub.ErrInvalidSubscription)
	})

	t.Run("evento que no cumple el contrato", func(t *testing.T) {
		err := bus.Publish(ctx, pubsub.Envelope{EventType: "mal"})

		assert.ErrorIs(t, err, pubsub.ErrInvalidEnvelope)
	})
}

func TestBus_ResetKeepsSubscriptions(t *testing.T) {
	ctx := context.Background()
	bus := pubsubtest.New()
	var received []pubsub.Envelope
	require.NoError(t, bus.Subscribe(ctx, pubsub.Subscription{
		Queue: pubsub.PerInstanceQueue(), BindingKeys: []string{"#"},
	}, collector(&received)))
	require.NoError(t, bus.Publish(ctx, event(t, "community.member.joined")))

	bus.Reset()

	assert.Empty(t, bus.Published())
	assert.Empty(t, bus.DeadLettered())
	require.NoError(t, bus.Publish(ctx, event(t, "community.member.joined")))
	assert.Len(t, bus.Published(), 1)
	assert.Len(t, received, 2, "resetear lo registrado no da de baja a los consumers")
}
