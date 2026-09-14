import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from discordia_pubsub import (
    Client,
    ClosedError,
    Config,
    Envelope,
    InvalidEnvelopeError,
    PermanentError,
    Subscription,
    new_envelope,
    per_instance_queue,
    shared_queue,
)
from discordia_pubsub.topology import InvalidSubscriptionError


def a_config(**overrides) -> Config:
    base = {
        "url": "amqp://localhost:5672/",
        "exchange": "discordia.events",
        "dead_letter_exchange": "discordia.events.dlx",
        "service_name": "community",
        "max_retries": 0,
        "retry_initial_delay": 0.001,
        "retry_max_delay": 0.002,
    }
    return Config(**{**base, **overrides})


@dataclass
class FakeExchange:
    name: str
    published: list[tuple[Any, dict]] = field(default_factory=list)

    async def publish(self, message, routing_key, **kwargs):
        self.published.append((message, {"routing_key": routing_key, **kwargs}))


@dataclass
class FakeQueue:
    name: str = "amq.gen-Jz2Kx9"
    consumers: list[dict] = field(default_factory=list)

    async def bind(self, exchange, routing_key=None, **_):
        pass

    async def consume(self, callback, no_ack=False, exclusive=False, **_):
        self.consumers.append({"callback": callback, "no_ack": no_ack, "exclusive": exclusive})
        return "consumer-tag"


@dataclass
class FakeChannel:
    exchanges: list[FakeExchange] = field(default_factory=list)
    queues: list[FakeQueue] = field(default_factory=list)
    qos: list[int] = field(default_factory=list)

    async def declare_exchange(self, name, type="direct", durable=False, **_):
        exchange = FakeExchange(name=name)
        self.exchanges.append(exchange)
        return exchange

    async def declare_queue(self, name=None, **_):
        queue = FakeQueue(name=name or "amq.gen-Jz2Kx9")
        self.queues.append(queue)
        return queue

    async def set_qos(self, prefetch_count=0, **_):
        self.qos.append(prefetch_count)


@dataclass
class FakeConnection:
    url: str = ""
    client_properties: dict = field(default_factory=dict)
    channels: list[FakeChannel] = field(default_factory=list)
    publisher_confirms: list[bool] = field(default_factory=list)
    closed: bool = False

    async def channel(self, publisher_confirms=True, **_):
        self.publisher_confirms.append(publisher_confirms)
        channel = FakeChannel()
        self.channels.append(channel)
        return channel

    async def close(self):
        self.closed = True


def fake_factory(connection: FakeConnection):
    async def factory(url, client_properties=None, **_):
        connection.url = url
        connection.client_properties = client_properties or {}
        return connection

    return factory


@dataclass
class FakeMessage:
    body: bytes
    message_id: str = "m1"
    acked: bool = False
    nacked: list[bool] = field(default_factory=list)

    async def ack(self):
        self.acked = True

    async def nack(self, requeue=False):
        self.nacked.append(requeue)


async def a_client(config: Config | None = None) -> tuple[Client, FakeConnection]:
    connection = FakeConnection()
    client = await Client.connect(config or a_config(), connection_factory=fake_factory(connection))
    return client, connection


def an_event() -> Envelope:
    return new_envelope("community.member.joined", "community", {"userId": "u1"})


async def test_connect_names_the_connection_and_declares_the_exchanges():
    client, connection = await a_client()

    assert connection.url == "amqp://localhost:5672/"
    assert connection.client_properties == {"connection_name": "community"}
    assert connection.publisher_confirms == [True], "sin confirms, un evento rechazado se pierde en silencio"
    assert [e.name for e in connection.channels[0].exchanges] == ["discordia.events", "discordia.events.dlx"]
    assert client.config.service_name == "community"


async def test_connect_rejects_an_invalid_config():
    with pytest.raises(Exception):
        await Client.connect(a_config(service_name=""), connection_factory=fake_factory(FakeConnection()))


async def test_publish_carries_the_envelope_identifiers_in_the_properties():
    client, connection = await a_client()
    event = an_event()

    await client.publish(event)

    message, options = connection.channels[0].exchanges[0].published[0]
    assert options["routing_key"] == "community.member.joined"
    assert options["mandatory"] is False, "que nadie esté escuchando todavía no es un error del publicador"
    assert message.message_id == event.event_id
    assert message.correlation_id == event.correlation_id
    assert message.type == "community.member.joined"
    assert message.app_id == "community"
    assert message.content_type == "application/json"
    assert Envelope.from_json(message.body) == event


async def test_publish_validates_before_touching_the_broker():
    client, connection = await a_client()

    with pytest.raises(InvalidEnvelopeError):
        await client.publish(Envelope.from_dict({**an_event().to_dict(), "eventType": "mal"}))

    assert connection.channels[0].exchanges[0].published == []


async def test_publish_after_close():
    client, connection = await a_client()
    await client.close()

    assert connection.closed
    with pytest.raises(ClosedError):
        await client.publish(an_event())


async def test_close_is_idempotent():
    client, _ = await a_client()

    await client.close()
    await client.close()


async def test_client_works_as_a_context_manager():
    connection = FakeConnection()

    async with await Client.connect(a_config(), connection_factory=fake_factory(connection)):
        pass

    assert connection.closed


async def test_subscribe_uses_its_own_channel_and_manual_ack():
    client, connection = await a_client()

    queue = await client.subscribe(
        Subscription(queue=per_instance_queue(), binding_keys=["chat.message.sent"]),
        lambda event: asyncio.sleep(0),
    )

    assert len(connection.channels) == 2, "el canal de publicación no se comparte con el consumer"
    consumer = queue.consumers[0]
    assert consumer["no_ack"] is False, "el ack va después de procesar, no al recibir"
    assert consumer["exclusive"] is True, "una cola por instancia se consume en exclusiva"


async def test_subscribe_on_a_shared_queue_is_not_exclusive():
    client, _ = await a_client()

    queue = await client.subscribe(
        Subscription(queue=shared_queue("community.projection"), binding_keys=["identity.#"]),
        lambda event: asyncio.sleep(0),
    )

    assert queue.consumers[0]["exclusive"] is False


async def test_subscribe_rejects_invalid_input():
    client, _ = await a_client()

    with pytest.raises(InvalidSubscriptionError):
        await client.subscribe(Subscription(queue=per_instance_queue()), lambda event: asyncio.sleep(0))

    with pytest.raises(ValueError):
        await client.subscribe(Subscription(queue=per_instance_queue(), binding_keys=["chat.#"]), None)


async def test_subscribe_after_close():
    client, _ = await a_client()
    await client.close()

    with pytest.raises(ClosedError):
        await client.subscribe(
            Subscription(queue=per_instance_queue(), binding_keys=["chat.#"]),
            lambda event: asyncio.sleep(0),
        )


async def callback_for(client: Client, handler) -> Any:
    queue = await client.subscribe(
        Subscription(queue=shared_queue("community.projection"), binding_keys=["community.#"]),
        handler,
    )
    return queue.consumers[0]["callback"]


async def test_a_processed_event_is_acked():
    client, _ = await a_client()
    seen = []
    callback = await callback_for(client, lambda event: seen.append(event) or asyncio.sleep(0))
    message = FakeMessage(body=an_event().to_json())

    await callback(message)

    assert message.acked
    assert message.nacked == []
    assert len(seen) == 1


async def test_a_permanent_failure_is_dead_lettered():
    client, _ = await a_client()

    async def handler(_event):
        raise PermanentError("channel does not belong to the server")

    callback = await callback_for(client, handler)
    message = FakeMessage(body=an_event().to_json())

    await callback(message)

    assert not message.acked
    assert message.nacked == [False], "sin requeue: va a la dead-letter queue"


async def test_an_exhausted_transient_failure_is_dead_lettered():
    client, _ = await a_client(a_config(max_retries=2))
    calls = 0

    async def handler(_event):
        nonlocal calls
        calls += 1
        raise RuntimeError("projection is down")

    callback = await callback_for(client, handler)
    message = FakeMessage(body=an_event().to_json())

    await callback(message)

    assert calls == 3, "el intento original más dos reintentos"
    assert message.nacked == [False]


async def test_a_malformed_message_never_reaches_the_handler():
    client, _ = await a_client()
    seen = []
    callback = await callback_for(client, lambda event: seen.append(event) or asyncio.sleep(0))
    message = FakeMessage(body=b'{"no soy":"un evento"}')

    await callback(message)

    assert seen == []
    assert message.nacked == [False], "un mensaje que no cumple el contrato no se vuelve válido por insistir"


async def test_cancellation_requeues_the_message():
    client, _ = await a_client()

    async def handler(_event):
        raise asyncio.CancelledError()

    callback = await callback_for(client, handler)
    message = FakeMessage(body=an_event().to_json())

    with pytest.raises(asyncio.CancelledError):
        await callback(message)

    assert message.nacked == [True], "nunca se llegó a procesar: vuelve a la cola"
