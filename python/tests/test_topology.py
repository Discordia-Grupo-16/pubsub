from dataclasses import dataclass, field
from typing import Any

import pytest

from discordia_pubsub.config import Config
from discordia_pubsub.topology import (
    InvalidSubscriptionError,
    Subscription,
    declare_exchanges,
    declare_subscription,
    per_instance_queue,
    shared_queue,
)


def a_config() -> Config:
    return Config(
        url="amqp://localhost:5672/",
        exchange="discordia.events",
        dead_letter_exchange="discordia.events.dlx",
        service_name="community",
    )


# Verificar la topología contra un doble en vez de contra un broker permite
# que el test que protege el riesgo principal —confundir cola compartida con
# cola por instancia— corra siempre, también sin RabbitMQ levantado.
@dataclass
class FakeQueue:
    name: str
    durable: bool
    exclusive: bool
    auto_delete: bool
    arguments: dict[str, Any] | None
    bindings: list[tuple[str, str]] = field(default_factory=list)

    async def bind(self, exchange, routing_key=None, **_):
        self.bindings.append((exchange, routing_key))


@dataclass
class FakeChannel:
    exchanges: list[dict] = field(default_factory=list)
    queues: list[FakeQueue] = field(default_factory=list)
    qos: list[int] = field(default_factory=list)

    async def declare_exchange(self, name, type="direct", durable=False, **_):
        self.exchanges.append({"name": name, "type": type, "durable": durable})
        return name

    async def declare_queue(self, name=None, durable=False, exclusive=False, auto_delete=False, arguments=None, **_):
        queue = FakeQueue(
            # El broker genera el nombre de las colas anónimas.
            name=name or "amq.gen-Jz2Kx9",
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
        )
        self.queues.append(queue)
        return queue

    async def set_qos(self, prefetch_count=0, **_):
        self.qos.append(prefetch_count)

    def queue_named(self, name: str) -> FakeQueue:
        for queue in self.queues:
            if queue.name == name:
                return queue
        raise AssertionError(f"no se declaró la cola {name!r}; declaradas: {[q.name for q in self.queues]}")


async def test_declares_a_durable_topic_exchange_and_its_dlx():
    channel = FakeChannel()

    await declare_exchanges(channel, a_config())

    assert channel.exchanges == [
        {"name": "discordia.events", "type": "topic", "durable": True},
        {"name": "discordia.events.dlx", "type": "topic", "durable": True},
    ]


async def test_shared_queue_is_durable_and_competes():
    channel = FakeChannel()
    subscription = Subscription(
        queue=shared_queue("community.identity-projection"),
        binding_keys=["identity.user.registered"],
    )

    await declare_subscription(channel, a_config(), subscription)

    queue = channel.queue_named("community.identity-projection")
    assert queue.durable, "una proyección no puede perder eventos porque se reinició el broker"
    assert not queue.exclusive, "las réplicas tienen que compartir la cola para repartirse los eventos"
    assert not queue.auto_delete
    assert queue.arguments == {
        "x-dead-letter-exchange": "discordia.events.dlx",
        "x-dead-letter-routing-key": "community.identity-projection.dlq",
    }


async def test_shared_queue_gets_its_own_dead_letter_queue():
    channel = FakeChannel()
    subscription = Subscription(queue=shared_queue("community.projection"), binding_keys=["identity.#"])

    await declare_subscription(channel, a_config(), subscription)

    dlq = channel.queue_named("community.projection.dlq")
    assert dlq.durable, "un evento muerto que se pierde al reiniciar no se puede investigar"
    assert dlq.bindings == [("discordia.events.dlx", "community.projection.dlq")]


async def test_per_instance_queue_is_exclusive_and_ephemeral():
    channel = FakeChannel()
    subscription = Subscription(queue=per_instance_queue(), binding_keys=["chat.message.sent"])

    queue = await declare_subscription(channel, a_config(), subscription)

    assert len(channel.queues) == 1, "una cola efímera no necesita DLQ"
    assert queue.name == "amq.gen-Jz2Kx9", "el nombre lo genera el broker"
    assert queue.exclusive, "sin exclusive, dos instancias compartirían la cola y el fan-out se rompe"
    assert queue.auto_delete
    assert not queue.durable
    assert queue.arguments is None


async def test_the_two_semantics_are_not_interchangeable():
    shared_channel, per_instance_channel = FakeChannel(), FakeChannel()
    keys = ["chat.message.sent"]

    await declare_subscription(shared_channel, a_config(), Subscription(queue=shared_queue("chat.q"), binding_keys=keys))
    await declare_subscription(per_instance_channel, a_config(), Subscription(queue=per_instance_queue(), binding_keys=keys))

    shared = shared_channel.queue_named("chat.q")
    ephemeral = per_instance_channel.queues[0]

    assert not shared.exclusive and ephemeral.exclusive
    assert shared.durable and not ephemeral.durable
    assert not shared.auto_delete and ephemeral.auto_delete


async def test_binds_every_key_to_the_events_exchange():
    channel = FakeChannel()
    subscription = Subscription(
        queue=per_instance_queue(),
        binding_keys=["chat.message.sent", "chat.message.deleted"],
    )

    queue = await declare_subscription(channel, a_config(), subscription)

    assert queue.bindings == [
        ("discordia.events", "chat.message.sent"),
        ("discordia.events", "chat.message.deleted"),
    ]


@pytest.mark.parametrize("prefetch,expected", [(0, 16), (64, 64)])
async def test_prefetch(prefetch, expected):
    channel = FakeChannel()
    subscription = Subscription(queue=per_instance_queue(), binding_keys=["chat.#"], prefetch=prefetch)

    await declare_subscription(channel, a_config(), subscription)

    assert channel.qos == [expected]


@pytest.mark.parametrize(
    "subscription",
    [
        Subscription(queue=shared_queue("community.projection"), binding_keys=["identity.user.registered"]),
        Subscription(queue=per_instance_queue(), binding_keys=["identity.user.*"]),
        Subscription(queue=per_instance_queue(), binding_keys=["identity.#"]),
        Subscription(queue=shared_queue("metrics.all"), binding_keys=["#"]),
        Subscription(queue=per_instance_queue(), binding_keys=["chat.#"], prefetch=32),
    ],
)
def test_valid_subscriptions(subscription):
    subscription.validate()


@pytest.mark.parametrize(
    "subscription",
    [
        Subscription(queue=shared_queue(""), binding_keys=["identity.#"]),
        Subscription(queue=shared_queue("community.projection")),
        Subscription(queue=shared_queue("community.projection"), binding_keys=[""]),
        Subscription(queue=shared_queue("community.projection"), binding_keys=["Identity.User.Registered"]),
        Subscription(queue=shared_queue("community.projection"), binding_keys=["identity..registered"]),
        Subscription(queue=per_instance_queue(), binding_keys=["chat.#"], prefetch=-1),
        Subscription(queue="no soy una QueueSpec", binding_keys=["chat.#"]),
    ],
)
def test_invalid_subscriptions(subscription):
    with pytest.raises(InvalidSubscriptionError):
        subscription.validate()


async def test_an_invalid_subscription_never_reaches_the_broker():
    channel = FakeChannel()

    with pytest.raises(InvalidSubscriptionError):
        await declare_subscription(channel, a_config(), Subscription(queue=per_instance_queue()))

    assert channel.queues == []


def test_queue_spec_describes_itself():
    assert str(shared_queue("community.projection")) == "shared queue community.projection"
    assert str(per_instance_queue()) == "per-instance queue"


def test_the_queue_semantics_has_to_be_chosen():
    with pytest.raises(TypeError):
        Subscription(binding_keys=["chat.#"])
