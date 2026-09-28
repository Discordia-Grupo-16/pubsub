"""Topología de colas sobre el exchange de eventos.

Un único topic exchange durable, `discordia.events`, con la routing key igual
al `eventType`, y dos semánticas de cola que **no** son intercambiables. Es
la misma topología que declara el cliente Go, así que un servicio Python y
uno Go que consuman el mismo evento se comportan igual.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from discordia_pubsub.config import Config

# Sufijo con el que se nombra la DLQ de una cola compartida, para que en la
# management UI queden una al lado de la otra.
DEAD_LETTER_QUEUE_SUFFIX = ".dlq"

# Acepta las routing keys del contrato de eventos y los comodines de un topic
# exchange: `*` (un segmento) y `#` (cero o más).
_BINDING_KEY_PATTERN = re.compile(r"^(?:[a-z][a-z0-9-]*|\*|#)(?:\.(?:[a-z][a-z0-9-]*|\*|#))*$")


class InvalidSubscriptionError(ValueError):
    """La subscripción no está bien definida."""


@dataclass(frozen=True, slots=True)
class QueueSpec:
    """Con qué semántica se declara la cola de una subscripción.

    No tiene valores por defecto a propósito: la elección se hace con
    `shared_queue` o `per_instance_queue`, explícitamente, porque
    equivocarla no da error, solo hace que a algunos usuarios no les llegue
    nada.
    """

    name: str
    per_instance: bool

    def __str__(self) -> str:
        return "per-instance queue" if self.per_instance else f"shared queue {self.name}"


def shared_queue(name: str) -> QueueSpec:
    """Cola durable con nombre fijo, compartida por todas las réplicas del
    servicio: el broker las reparte round-robin y cada evento lo procesa una
    sola.

    Es lo que corresponde a todo lo que termina escribiendo en la base del
    servicio: no tiene sentido que tres instancias apliquen el mismo evento
    tres veces.
    """
    return QueueSpec(name=name, per_instance=False)


def per_instance_queue() -> QueueSpec:
    """Cola exclusive y auto-delete con nombre generado por el broker: cada
    instancia recibe *todos* los eventos y la cola desaparece cuando la
    instancia se va.

    Es el único modo que hace funcionar el fan-out multiinstancia.
    """
    return QueueSpec(name="", per_instance=True)


@dataclass(frozen=True, slots=True)
class Subscription:
    """Qué eventos consume un servicio y con qué semántica."""

    queue: QueueSpec
    binding_keys: tuple[str, ...] | list[str] = field(default_factory=tuple)
    prefetch: int = 0

    def validate(self) -> None:
        if not isinstance(self.queue, QueueSpec):
            raise InvalidSubscriptionError("queue must be shared_queue(name) or per_instance_queue()")
        if not self.queue.per_instance and not self.queue.name:
            raise InvalidSubscriptionError("a shared queue needs a name")
        if not self.binding_keys:
            raise InvalidSubscriptionError(
                "at least one binding key is required, a queue with no bindings never receives anything"
            )
        for key in self.binding_keys:
            if not _BINDING_KEY_PATTERN.match(key or ""):
                raise InvalidSubscriptionError(f"binding key {key!r} is not a valid topic pattern")
        if self.prefetch < 0:
            raise InvalidSubscriptionError(f"prefetch must be >= 0, got {self.prefetch}")

    def effective_prefetch(self, config: Config) -> int:
        return self.prefetch if self.prefetch > 0 else config.prefetch


async def declare_exchanges(channel: Any, config: Config) -> Any:
    """Declara el exchange de eventos y su dead-letter exchange, y devuelve
    el primero. Es idempotente."""
    exchange = await channel.declare_exchange(config.exchange, "topic", durable=True)
    await channel.declare_exchange(config.dead_letter_exchange, "topic", durable=True)
    return exchange


async def declare_subscription(channel: Any, config: Config, subscription: Subscription) -> Any:
    """Declara la cola de la subscripción con la semántica que corresponda,
    la ata al exchange por cada binding key y fija el prefetch."""
    subscription.validate()

    await channel.set_qos(prefetch_count=subscription.effective_prefetch(config))

    if subscription.queue.per_instance:
        # Sin durabilidad ni dead-lettering: si la instancia muere, su cola
        # se va con ella. Lo que consume una cola por instancia es fan-out en
        # vivo, y lo que se pierde se recupera del historial persistido, así
        # que acumular eventos muertos de instancias que ya no existen sería
        # basura, no resiliencia.
        queue = await channel.declare_queue(None, durable=False, exclusive=True, auto_delete=True)
    else:
        dead_letter_queue = subscription.queue.name + DEAD_LETTER_QUEUE_SUFFIX
        dlq = await channel.declare_queue(dead_letter_queue, durable=True)
        await dlq.bind(config.dead_letter_exchange, routing_key=dead_letter_queue)

        queue = await channel.declare_queue(
            subscription.queue.name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": config.dead_letter_exchange,
                "x-dead-letter-routing-key": dead_letter_queue,
            },
        )

    for key in subscription.binding_keys:
        await queue.bind(config.exchange, routing_key=key)

    return queue
