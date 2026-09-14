"""Política de reintentos de un handler."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Awaitable, Callable

from discordia_pubsub.config import Config
from discordia_pubsub.envelope import Envelope
from discordia_pubsub.errors import is_permanent

# Handler procesa un evento. Terminar sin excepción ackea el mensaje; levantar
# una excepción cualquiera lo reintenta; levantar PermanentError lo manda a la
# dead-letter queue sin reintentos.
#
# El bus entrega at-least-once: todo handler puede recibir el mismo evento más
# de una vez y tiene que tolerarlo. La idempotencia es responsabilidad del
# handler; el cliente no la puede resolver por él.
Handler = Callable[[Envelope], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Cuánto esperar entre reintentos de un mismo evento.

    El backoff es exponencial con jitter: sin jitter, N instancias que fallan
    por la misma causa —una base caída, por ejemplo— reintentan todas en el
    mismo instante y le pegan al recurso justo cuando se está recuperando.
    """

    max_retries: int
    initial_delay: float
    max_delay: float

    @classmethod
    def from_config(cls, config: Config) -> RetryPolicy:
        return cls(
            max_retries=config.max_retries,
            initial_delay=config.retry_initial_delay,
            max_delay=config.retry_max_delay,
        )

    def delay(self, attempt: int) -> float:
        """Espera antes del reintento número `attempt` (1 es el primero).

        Cae entre la mitad y el total del backoff exponencial de ese intento,
        acotada por el máximo configurado.
        """
        attempt = max(attempt, 1)

        backoff = self.initial_delay
        for _ in range(attempt - 1):
            if backoff >= self.max_delay:
                break
            backoff *= 2
        backoff = min(backoff, self.max_delay)

        return backoff / 2 + random.uniform(0, backoff / 2)

    async def wait(self, attempt: int) -> None:
        await asyncio.sleep(self.delay(attempt))


async def run_with_retries(handler: Handler, event: Envelope, policy: RetryPolicy) -> None:
    """Ejecuta el handler y lo reintenta mientras el fallo sea transitorio.

    Levanta la última excepción si se agotan los reintentos. CancelledError
    no se atrapa: un shutdown corta el ciclo en vez de seguir reintentando.
    """
    attempt = 0
    while True:
        try:
            await handler(event)
            return
        except Exception as exc:
            if is_permanent(exc) or attempt >= policy.max_retries:
                raise
            attempt += 1
            await policy.wait(attempt)
