"""Bus en memoria para testear handlers sin levantar RabbitMQ.

Tiene la misma API y la misma semántica de reparto que el cliente real:

    bus = InMemoryBus()
    await bus.subscribe(subscription, projection.handle)
    await bus.publish(event)
    # acá el handler ya corrió: la entrega es sincrónica
    assert bus.dead_lettered == []
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from discordia_pubsub.envelope import Envelope
from discordia_pubsub.errors import is_permanent
from discordia_pubsub.retry import Handler
from discordia_pubsub.topology import QueueSpec, Subscription


@dataclass
class _Consumer:
    subscription: Subscription
    handler: Handler


@dataclass
class _Queue:
    """Una cola del bus. Las subscripciones a una misma cola compartida se
    reparten los eventos; cada cola por instancia es su propia cola y recibe
    todo."""

    name: str
    per_instance: bool
    consumers: list[_Consumer] = field(default_factory=list)
    next_consumer: int = 0


class InMemoryBus:
    """Bus en memoria. La entrega es sincrónica: cuando `publish` vuelve,
    todos los handlers que correspondían ya corrieron, así que un test no
    necesita esperas ni polling."""

    def __init__(self, max_retries: int = 0) -> None:
        # Reintentos ante un fallo transitorio, sin esperas entre uno y otro.
        # Por defecto cero: un intento y a la dead-letter.
        self.max_retries = max_retries
        self._queues: list[_Queue] = []
        self._published: list[Envelope] = []
        self._dead_lettered: list[Envelope] = []

    async def subscribe(self, subscription: Subscription, handler: Handler) -> None:
        """Registra un consumer. Valida la subscripción igual que el cliente
        real, para que un binding key mal escrito falle en el test y no en
        producción."""
        subscription.validate()
        if handler is None:
            raise ValueError("handler is required")

        self._queue_for(subscription.queue).consumers.append(_Consumer(subscription, handler))

    async def publish(self, event: Envelope) -> None:
        """Valida el evento, lo registra y se lo entrega a los consumers que
        lo tengan bindeado."""
        event.validate()
        self._published.append(event)

        for consumer in self._targets_for(event):
            try:
                await self._deliver(consumer, event)
            except Exception:
                self._dead_lettered.append(event)

    @property
    def published(self) -> list[Envelope]:
        """Todos los eventos publicados, en orden."""
        return list(self._published)

    def published_of(self, event_type: str) -> list[Envelope]:
        """Los eventos publicados de un tipo. Es la forma habitual de
        verificar que una operación emitió el evento que tenía que emitir."""
        return [event for event in self._published if event.event_type == event_type]

    @property
    def dead_lettered(self) -> list[Envelope]:
        """Los eventos cuyo handler falló hasta agotar los reintentos. En un
        test que espera que todo ande, tiene que estar vacío."""
        return list(self._dead_lettered)

    def reset(self) -> None:
        """Vacía lo registrado sin dar de baja a los consumers."""
        self._published.clear()
        self._dead_lettered.clear()

    async def _deliver(self, consumer: _Consumer, event: Envelope) -> None:
        attempt = 0
        while True:
            try:
                await consumer.handler(event)
                return
            except Exception as exc:
                if is_permanent(exc) or attempt >= self.max_retries:
                    raise
                attempt += 1

    def _queue_for(self, spec: QueueSpec) -> _Queue:
        """Devuelve la cola de la subscripción, replicando la diferencia que
        hace el broker: las réplicas de un servicio comparten la cola con
        nombre y compiten por cada evento, mientras que cada cola por
        instancia es independiente y recibe todo."""
        if not spec.per_instance:
            for queue in self._queues:
                if not queue.per_instance and queue.name == spec.name:
                    return queue

        created = _Queue(name=spec.name, per_instance=spec.per_instance)
        self._queues.append(created)
        return created

    def _targets_for(self, event: Envelope) -> list[_Consumer]:
        """Elige un consumer por cola: en una cola compartida el evento va a
        uno solo, rotando entre los que haya, igual que el round-robin del
        broker."""
        targets: list[_Consumer] = []

        for queue in self._queues:
            eligible = [c for c in queue.consumers if _matches_any(c.subscription.binding_keys, event.event_type)]
            if not eligible:
                continue

            if queue.per_instance:
                targets.extend(eligible)
                continue

            targets.append(eligible[queue.next_consumer % len(eligible)])
            queue.next_consumer += 1

        return targets


def _matches_any(binding_keys: Any, event_type: str) -> bool:
    return any(_matches_topic(key.split("."), event_type.split(".")) for key in binding_keys)


def _matches_topic(pattern: list[str], segments: list[str]) -> bool:
    """Matching de un topic exchange: `*` es exactamente un segmento y `#` es
    cero o más."""
    if not pattern:
        return not segments

    head = pattern[0]
    if head == "#":
        return any(_matches_topic(pattern[1:], segments[i:]) for i in range(len(segments) + 1))
    if head == "*":
        return bool(segments) and _matches_topic(pattern[1:], segments[1:])
    return bool(segments) and segments[0] == head and _matches_topic(pattern[1:], segments[1:])
