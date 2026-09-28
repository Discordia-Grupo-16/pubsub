"""Lo que un servicio necesita del cliente de Pub/Sub.

Tipar contra este Protocol en vez de contra Client es lo que permite
sustituirlo por el bus en memoria de `discordia_pubsub.testing` en los tests
del servicio.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from discordia_pubsub.envelope import Envelope
from discordia_pubsub.retry import Handler
from discordia_pubsub.topology import Subscription


@runtime_checkable
class Bus(Protocol):
    async def publish(self, event: Envelope) -> None: ...

    async def subscribe(self, subscription: Subscription, handler: Handler) -> Any: ...
