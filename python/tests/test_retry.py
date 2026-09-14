import asyncio

import pytest

from discordia_pubsub import new_envelope
from discordia_pubsub.config import Config
from discordia_pubsub.errors import PermanentError
from discordia_pubsub.retry import RetryPolicy, run_with_retries


class TransientError(Exception):
    pass


def policy(max_retries: int = 3) -> RetryPolicy:
    return RetryPolicy(max_retries=max_retries, initial_delay=0.2, max_delay=5.0)


def fast_policy(max_retries: int = 3) -> RetryPolicy:
    return RetryPolicy(max_retries=max_retries, initial_delay=0.001, max_delay=0.002)


def an_event():
    return new_envelope("community.member.joined", "community", {"userId": "u1"})


@pytest.mark.parametrize("attempt,expected", [(1, 0.2), (2, 0.4), (3, 0.8), (4, 1.6)])
def test_delay_grows_exponentially_within_jitter_bounds(attempt, expected):
    for _ in range(50):
        delay = policy().delay(attempt)

        assert expected / 2 <= delay <= expected


@pytest.mark.parametrize("attempt", [10, 100, 1000])
def test_delay_is_capped(attempt):
    delay = policy().delay(attempt)

    assert 2.5 <= delay <= 5.0, "un backoff sin techo deja eventos parados horas"


def test_delay_jitters():
    delays = {policy().delay(3) for _ in range(100)}

    assert len(delays) > 1, "sin jitter, N instancias reintentan todas en el mismo instante"


@pytest.mark.parametrize("attempt", [0, -1])
def test_delay_normalizes_invalid_attempts(attempt):
    assert 0.1 <= policy().delay(attempt) <= 0.2


def test_from_config():
    config = Config(
        url="amqp://localhost:5672/",
        exchange="discordia.events",
        dead_letter_exchange="discordia.events.dlx",
        service_name="community",
        max_retries=5,
        retry_initial_delay=1.0,
        retry_max_delay=60.0,
    )

    assert RetryPolicy.from_config(config) == RetryPolicy(max_retries=5, initial_delay=1.0, max_delay=60.0)


async def test_succeeds_on_first_try():
    calls = 0

    async def handler(_):
        nonlocal calls
        calls += 1

    await run_with_retries(handler, an_event(), fast_policy())

    assert calls == 1


async def test_retries_transient_failures():
    calls = 0

    async def handler(_):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TransientError("projection is down")

    await run_with_retries(handler, an_event(), fast_policy())

    assert calls == 3


async def test_gives_up_after_max_retries():
    calls = 0

    async def handler(_):
        nonlocal calls
        calls += 1
        raise TransientError("projection is down")

    with pytest.raises(TransientError):
        await run_with_retries(handler, an_event(), fast_policy(3))

    assert calls == 4, "el intento original más tres reintentos"


async def test_does_not_retry_permanent_failures():
    calls = 0

    async def handler(_):
        nonlocal calls
        calls += 1
        raise PermanentError("channel does not belong to the server")

    with pytest.raises(PermanentError):
        await run_with_retries(handler, an_event(), fast_policy(5))

    assert calls == 1


async def test_zero_retries():
    calls = 0

    async def handler(_):
        nonlocal calls
        calls += 1
        raise TransientError("nope")

    with pytest.raises(TransientError):
        await run_with_retries(handler, an_event(), fast_policy(0))

    assert calls == 1, "cero reintentos es un intento y a la DLQ"


async def test_cancellation_stops_the_retry_loop():
    calls = 0
    started = asyncio.Event()

    async def handler(_):
        nonlocal calls
        calls += 1
        started.set()
        raise TransientError("nope")

    slow = RetryPolicy(max_retries=10, initial_delay=10.0, max_delay=60.0)
    task = asyncio.create_task(run_with_retries(handler, an_event(), slow))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert calls == 1, "un shutdown no espera a que se agoten los reintentos"


async def test_passes_the_event_through():
    event = an_event()
    received = None

    async def handler(e):
        nonlocal received
        received = e

    await run_with_retries(handler, event, fast_policy())

    assert received == event
