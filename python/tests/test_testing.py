import pytest

from discordia_pubsub import (
    Bus,
    Client,
    Envelope,
    InvalidEnvelopeError,
    PermanentError,
    Subscription,
    new_envelope,
    per_instance_queue,
    shared_queue,
)
from discordia_pubsub.testing import InMemoryBus
from discordia_pubsub.topology import InvalidSubscriptionError


def an_event(event_type: str = "community.member.joined") -> Envelope:
    return new_envelope(event_type, "community", {"serverId": "s1"})


def collector(received: list):
    async def handler(event):
        received.append(event)

    return handler


def test_both_implementations_satisfy_the_bus_protocol():
    assert issubclass(InMemoryBus, Bus)
    assert issubclass(Client, Bus)


async def test_delivers_synchronously():
    bus = InMemoryBus()
    received = []
    await bus.subscribe(
        Subscription(queue=shared_queue("chat.projection"), binding_keys=["community.member.joined"]),
        collector(received),
    )

    sent = an_event()
    await bus.publish(sent)

    assert received == [sent], "cuando publish vuelve, el handler ya corrió: el test no necesita esperas"


@pytest.mark.parametrize(
    "binding_key,event_type,delivered",
    [
        ("community.member.joined", "community.member.joined", True),
        ("community.member.joined", "community.member.left", False),
        ("community.member.*", "community.member.left", True),
        ("community.*", "community.member.left", False),
        ("community.#", "community.member.left", True),
        ("community.server.#", "community.server.created", True),
        ("#", "chat.message.sent", True),
        ("community.#.joined", "community.member.joined", True),
        ("community.#", "chat.message.sent", False),
    ],
)
async def test_routes_by_binding_key(binding_key, event_type, delivered):
    bus = InMemoryBus()
    received = []
    await bus.subscribe(
        Subscription(queue=per_instance_queue(), binding_keys=[binding_key]),
        collector(received),
    )

    await bus.publish(an_event(event_type))

    assert (len(received) == 1) is delivered


# Estas dos propiedades son la razón de ser del fake: reproducen la
# diferencia entre las dos semánticas de cola sin necesidad de un broker.
async def test_shared_queue_splits_events_between_replicas():
    bus = InMemoryBus()
    first, second = [], []
    subscription = Subscription(queue=shared_queue("chat.community-projection"), binding_keys=["community.#"])
    await bus.subscribe(subscription, collector(first))
    await bus.subscribe(subscription, collector(second))

    for _ in range(4):
        await bus.publish(an_event())

    assert len(first) == 2
    assert len(second) == 2, "las réplicas compiten por la cola: cada evento lo procesa una sola"


async def test_per_instance_queue_fans_out_to_every_instance():
    bus = InMemoryBus()
    first, second = [], []
    subscription = Subscription(queue=per_instance_queue(), binding_keys=["chat.message.sent"])
    await bus.subscribe(subscription, collector(first))
    await bus.subscribe(subscription, collector(second))

    await bus.publish(an_event("chat.message.sent"))

    assert len(first) == 1
    assert len(second) == 1, "es el fan-out que el CP1 tiene que demostrar"


async def test_different_shared_queues_each_get_the_event():
    bus = InMemoryBus()
    chat, metrics = [], []
    await bus.subscribe(Subscription(queue=shared_queue("chat.projection"), binding_keys=["community.#"]), collector(chat))
    await bus.subscribe(Subscription(queue=shared_queue("metrics.all"), binding_keys=["#"]), collector(metrics))

    await bus.publish(an_event())

    assert len(chat) == 1
    assert len(metrics) == 1, "dos servicios distintos reciben el mismo evento cada uno en su cola"


async def test_records_what_was_published():
    bus = InMemoryBus()

    await bus.publish(an_event("community.server.created"))
    await bus.publish(an_event("community.member.joined"))

    assert len(bus.published) == 2
    assert len(bus.published_of("community.member.joined")) == 1
    assert bus.published_of("chat.message.sent") == []


async def test_records_dead_lettered_events():
    bus = InMemoryBus()

    async def failing(_event):
        raise RuntimeError("projection is down")

    await bus.subscribe(Subscription(queue=shared_queue("chat.projection"), binding_keys=["#"]), failing)

    await bus.publish(an_event())

    assert len(bus.dead_lettered) == 1, "un handler que falla deja rastro"


async def test_retries_transient_failures():
    bus = InMemoryBus(max_retries=2)
    calls = 0

    async def flaky(_event):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("projection is down")

    await bus.subscribe(Subscription(queue=shared_queue("chat.projection"), binding_keys=["#"]), flaky)

    await bus.publish(an_event())

    assert calls == 3
    assert bus.dead_lettered == []


async def test_does_not_retry_permanent_failures():
    bus = InMemoryBus(max_retries=5)
    calls = 0

    async def permanent_failure(_event):
        nonlocal calls
        calls += 1
        raise PermanentError("not a member")

    await bus.subscribe(Subscription(queue=shared_queue("chat.projection"), binding_keys=["#"]), permanent_failure)

    await bus.publish(an_event())

    assert calls == 1
    assert len(bus.dead_lettered) == 1


async def test_validates_like_the_real_client():
    bus = InMemoryBus()

    with pytest.raises(InvalidSubscriptionError):
        await bus.subscribe(Subscription(queue=per_instance_queue()), collector([]))

    with pytest.raises(ValueError):
        await bus.subscribe(Subscription(queue=per_instance_queue(), binding_keys=["#"]), None)

    with pytest.raises(InvalidEnvelopeError):
        await bus.publish(Envelope.from_dict({**an_event().to_dict(), "eventType": "mal"}))


async def test_reset_keeps_subscriptions():
    bus = InMemoryBus()
    received = []
    await bus.subscribe(Subscription(queue=per_instance_queue(), binding_keys=["#"]), collector(received))
    await bus.publish(an_event())

    bus.reset()

    assert bus.published == []
    assert bus.dead_lettered == []
    await bus.publish(an_event())
    assert len(bus.published) == 1
    assert len(received) == 2, "resetear lo registrado no da de baja a los consumers"
