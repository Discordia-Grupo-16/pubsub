"""Tests contra un RabbitMQ real.

Se saltan solos si no hay ninguno alcanzable, para que `pytest` siga andando
sin Docker levantado:

    docker compose up -d
    RABBITMQ_URL=amqp://localhost:5672/ pytest

La cobertura que pide la DoD se mide con el broker arriba, que es como corre
CI.
"""

import asyncio
import inspect
import os
import uuid

import aio_pika
import pytest

from discordia_pubsub import (
    Client,
    Config,
    Envelope,
    PermanentError,
    Subscription,
    new_envelope,
    per_instance_queue,
    shared_queue,
)
from discordia_pubsub.topology import DEAD_LETTER_QUEUE_SUFFIX

TIMEOUT = 5.0


def broker_url() -> str:
    return os.environ.get("RABBITMQ_URL") or "amqp://localhost:5672/"


@pytest.fixture
async def config():
    """Config con exchange propio por test, para no pisarse con los demás ni
    dejar basura en el broker de quien lo corra en local."""
    try:
        probe = await asyncio.wait_for(aio_pika.connect(broker_url()), timeout=2)
    except Exception as exc:  # noqa: BLE001 - cualquier fallo de conexión salta el test
        pytest.skip(f"no hay un RabbitMQ alcanzable en {broker_url()!r}: {exc}")
    await probe.close()

    suffix = uuid.uuid4().hex[:8]
    cfg = Config(
        url=broker_url(),
        exchange=f"discordia.events.test-{suffix}",
        dead_letter_exchange=f"discordia.events.test-{suffix}.dlx",
        service_name="pubsub-itest",
        prefetch=8,
        max_retries=2,
        retry_initial_delay=0.01,
        retry_max_delay=0.05,
        publish_timeout=TIMEOUT,
    )

    yield cfg

    connection = await aio_pika.connect(cfg.url)
    async with connection:
        channel = await connection.channel()
        for name in _declared_queues:
            await channel.queue_delete(name)
            await channel.queue_delete(name + DEAD_LETTER_QUEUE_SUFFIX)
        _declared_queues.clear()
        await channel.exchange_delete(cfg.exchange)
        await channel.exchange_delete(cfg.dead_letter_exchange)


_declared_queues: list[str] = []


def a_shared_queue():
    """Cola compartida con nombre único, que el fixture borra al terminar."""
    name = f"itest.{uuid.uuid4().hex[:8]}"
    _declared_queues.append(name)
    return shared_queue(name)


@pytest.fixture
async def client(config):
    client = await Client.connect(config)
    yield client
    await client.close()


def a_message() -> Envelope:
    return new_envelope("chat.message.sent", "chat", {"messageId": str(uuid.uuid4())})


async def wait_until(condition, reason: str) -> None:
    """Espera a que se cumpla la condición —sync o async— o falla el test."""
    try:
        async with asyncio.timeout(TIMEOUT):
            while True:
                result = condition()
                if inspect.isawaitable(result):
                    result = await result
                if result:
                    return
                await asyncio.sleep(0.01)
    except TimeoutError:
        pytest.fail(f"se acabó el tiempo esperando: {reason}")


def collector(received: list):
    async def handler(event):
        received.append(event)

    return handler


async def drain(config: Config, queue_name: str) -> list[bytes]:
    """Saca todo lo que haya en una cola sin dejarlo ahí."""
    bodies: list[bytes] = []
    connection = await aio_pika.connect(config.url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.get_queue(queue_name, ensure=False)
        while True:
            message = await queue.get(no_ack=True, fail=False)
            if message is None:
                return bodies
            bodies.append(message.body)


async def test_publish_and_consume(config, client):
    received = []
    await client.subscribe(
        Subscription(queue=a_shared_queue(), binding_keys=["chat.message.sent"]),
        collector(received),
    )

    sent = a_message()
    await client.publish(sent)

    await wait_until(lambda: len(received) == 1, "que llegue el evento")
    assert received[0] == sent, "el sobre sobrevive el viaje entero, occurredAt incluido"


async def test_binding_keys_filter_what_arrives(config, client):
    received = []
    await client.subscribe(
        Subscription(queue=a_shared_queue(), binding_keys=["community.member.*"]),
        collector(received),
    )

    await client.publish(a_message())
    await client.publish(new_envelope("community.member.joined", "community", {"userId": "u1"}))

    await wait_until(lambda: len(received) == 1, "que llegue el evento bindeado")
    await asyncio.sleep(0.2)

    assert [e.event_type for e in received] == ["community.member.joined"]


# La propiedad que el CP1 tiene que demostrar: dos instancias del servicio,
# cada una con sus propios clientes conectados, reciben las dos el mensaje.
async def test_per_instance_queues_fan_out_to_every_instance(config):
    first = await Client.connect(config)
    second = await Client.connect(config)
    first_inbox, second_inbox = [], []
    subscription = Subscription(queue=per_instance_queue(), binding_keys=["chat.message.sent"])

    try:
        await first.subscribe(subscription, collector(first_inbox))
        await second.subscribe(subscription, collector(second_inbox))

        sent = a_message()
        await first.publish(sent)

        await wait_until(
            lambda: len(first_inbox) == 1 and len(second_inbox) == 1,
            "que el mensaje llegue a las dos instancias",
        )
        assert first_inbox[0].event_id == sent.event_id == second_inbox[0].event_id
    finally:
        await first.close()
        await second.close()


# La contracara: sobre una cola compartida, el mismo evento lo procesa una
# sola réplica.
async def test_shared_queue_splits_events_between_replicas(config):
    first = await Client.connect(config)
    second = await Client.connect(config)
    first_inbox, second_inbox = [], []
    subscription = Subscription(queue=a_shared_queue(), binding_keys=["chat.message.sent"])

    try:
        await first.subscribe(subscription, collector(first_inbox))
        await second.subscribe(subscription, collector(second_inbox))

        sent = [a_message() for _ in range(10)]
        for event in sent:
            await first.publish(event)

        await wait_until(
            lambda: len(first_inbox) + len(second_inbox) == len(sent),
            "que se procesen todos los eventos",
        )
        await asyncio.sleep(0.2)

        delivered = [e.event_id for e in first_inbox + second_inbox]
        assert sorted(delivered) == sorted(e.event_id for e in sent), "ni duplicados ni perdidos"
        assert first_inbox and second_inbox, "los dos consumers trabajaron"
    finally:
        await first.close()
        await second.close()


async def test_transient_failure_is_retried_and_then_acked(config, client):
    queue = a_shared_queue()
    attempts = 0

    async def flaky(_event):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("projection is down")

    await client.subscribe(Subscription(queue=queue, binding_keys=["chat.message.sent"]), flaky)
    await client.publish(a_message())

    await wait_until(lambda: attempts == 3, "que el handler termine bien después de reintentar")
    assert await drain(config, queue.name + DEAD_LETTER_QUEUE_SUFFIX) == []


async def test_permanent_failure_goes_to_the_dead_letter_queue(config, client):
    queue = a_shared_queue()
    attempts = 0

    async def always_permanent(_event):
        nonlocal attempts
        attempts += 1
        raise PermanentError("channel does not belong to the server")

    await client.subscribe(Subscription(queue=queue, binding_keys=["chat.message.sent"]), always_permanent)
    sent = a_message()
    await client.publish(sent)

    dead: list[bytes] = []

    async def arrived() -> bool:
        dead.extend(await drain(config, queue.name + DEAD_LETTER_QUEUE_SUFFIX))
        return len(dead) == 1

    await wait_until(arrived, "que el evento aparezca en la DLQ")

    assert attempts == 1, "un fallo permanente no se reintenta"
    assert Envelope.from_json(dead[0]).event_id == sent.event_id


async def test_malformed_message_never_reaches_the_handler(config, client):
    queue = a_shared_queue()
    received = []
    await client.subscribe(
        Subscription(queue=queue, binding_keys=["chat.message.sent"]),
        collector(received),
    )

    # Lo que pasaría si alguien publicara al bus sin usar este cliente.
    connection = await aio_pika.connect(config.url)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.get_exchange(config.exchange)
        await exchange.publish(
            aio_pika.Message(body=b'{"no soy":"un evento"}', content_type="application/json"),
            routing_key="chat.message.sent",
        )

    dead: list[bytes] = []

    async def arrived() -> bool:
        dead.extend(await drain(config, queue.name + DEAD_LETTER_QUEUE_SUFFIX))
        return len(dead) == 1

    await wait_until(arrived, "que el mensaje malformado aparezca en la DLQ")

    assert received == [], "el handler nunca ve un mensaje que no cumple el contrato"


async def test_publish_after_close(config):
    client = await Client.connect(config)
    await client.close()

    with pytest.raises(Exception):
        await client.publish(a_message())
