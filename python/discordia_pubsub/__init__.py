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

__all__ = [
    "Config",
    "DEFAULT_EVENT_VERSION",
    "Envelope",
    "InvalidConfigError",
    "InvalidEnvelopeError",
    "load_config",
    "new_envelope",
    "with_data",
]
