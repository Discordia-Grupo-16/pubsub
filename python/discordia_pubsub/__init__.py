"""Cliente compartido de Pub/Sub de Discordia sobre RabbitMQ (INF-06).

Espejo en Python del cliente Go: mismo envelope, mismas variables de entorno
y las mismas dos semánticas de cola.
"""

from discordia_pubsub.bus import Bus
from discordia_pubsub.client import Client, connect
from discordia_pubsub.config import Config, InvalidConfigError, load_config
from discordia_pubsub.envelope import (
    DEFAULT_EVENT_VERSION,
    Envelope,
    InvalidEnvelopeError,
    new_envelope,
    with_data,
)
from discordia_pubsub.errors import ClosedError, PermanentError, is_permanent, permanent
from discordia_pubsub.retry import Handler, RetryPolicy
from discordia_pubsub.topology import (
    InvalidSubscriptionError,
    QueueSpec,
    Subscription,
    per_instance_queue,
    shared_queue,
)

__all__ = [
    "DEFAULT_EVENT_VERSION",
    "Bus",
    "Client",
    "ClosedError",
    "Config",
    "Envelope",
    "Handler",
    "InvalidConfigError",
    "InvalidEnvelopeError",
    "InvalidSubscriptionError",
    "PermanentError",
    "QueueSpec",
    "RetryPolicy",
    "Subscription",
    "connect",
    "is_permanent",
    "load_config",
    "new_envelope",
    "per_instance_queue",
    "permanent",
    "shared_queue",
    "with_data",
]
