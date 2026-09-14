"""Cliente compartido de Pub/Sub de Discordia sobre RabbitMQ (INF-06).

Espejo en Python del cliente Go: mismo envelope, mismas variables de entorno
y las mismas dos semánticas de cola.
"""

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

__all__ = [
    "DEFAULT_EVENT_VERSION",
    "ClosedError",
    "Config",
    "Envelope",
    "Handler",
    "InvalidConfigError",
    "InvalidEnvelopeError",
    "PermanentError",
    "RetryPolicy",
    "is_permanent",
    "load_config",
    "new_envelope",
    "permanent",
    "with_data",
]
