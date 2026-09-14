"""Configuración del cliente, leída de las mismas variables que el cliente Go.

Los dos clientes comparten nombres, defaults y formato de valores: un mismo
`.env` sirve para un servicio Python y para uno Go, y un cambio de política
de prefetch o de reintentos se aplica igual en los ocho servicios.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

# Valores por defecto. Los de topología vienen de ADR-0003; los de reintentos
# y prefetch son el pendiente que ese ADR delegó a INF-06 y quedan
# justificados en el README.
DEFAULT_EXCHANGE = "discordia.events"
DEFAULT_PREFETCH = 16
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_INITIAL_DELAY = 0.2
DEFAULT_RETRY_MAX_DELAY = 5.0
DEFAULT_PUBLISH_TIMEOUT = 5.0
DEFAULT_RECONNECT_INITIAL_DELAY = 0.5
DEFAULT_RECONNECT_MAX_DELAY = 30.0

_DEAD_LETTER_SUFFIX = ".dlx"

# El default no lleva credenciales a propósito: `guest:guest` es el default
# del propio RabbitMQ para conexiones locales, y así el repo no tiene ni
# siquiera credenciales de mentira escritas. En cualquier entorno que no sea
# local, RABBITMQ_URL las trae desde el sistema de secretos.
_DEFAULT_URL = "amqp://localhost:5672/"

_DURATION_PATTERN = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")
_DURATION_UNITS = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}


class InvalidConfigError(ValueError):
    """La configuración del cliente no es utilizable."""


@dataclass(frozen=True, slots=True)
class Config:
    """Configuración del cliente. Las duraciones van en segundos."""

    url: str
    exchange: str
    dead_letter_exchange: str
    service_name: str
    prefetch: int = DEFAULT_PREFETCH
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_initial_delay: float = DEFAULT_RETRY_INITIAL_DELAY
    retry_max_delay: float = DEFAULT_RETRY_MAX_DELAY
    publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT
    reconnect_initial_delay: float = DEFAULT_RECONNECT_INITIAL_DELAY
    reconnect_max_delay: float = DEFAULT_RECONNECT_MAX_DELAY

    def validate(self) -> None:
        if not self.url:
            raise InvalidConfigError("url is required")
        if not self.exchange:
            raise InvalidConfigError("exchange is required")
        if not self.dead_letter_exchange:
            raise InvalidConfigError("dead_letter_exchange is required")
        if self.dead_letter_exchange == self.exchange:
            raise InvalidConfigError(
                f"dead_letter_exchange must differ from exchange, both are {self.exchange!r}"
            )
        if not self.service_name:
            raise InvalidConfigError("service_name is required (set SERVICE_NAME)")
        if self.prefetch < 1:
            raise InvalidConfigError(f"prefetch must be >= 1, got {self.prefetch}")
        if self.max_retries < 0:
            raise InvalidConfigError(f"max_retries must be >= 0, got {self.max_retries}")
        if self.retry_initial_delay <= 0:
            raise InvalidConfigError(f"retry_initial_delay must be > 0, got {self.retry_initial_delay}")
        if self.retry_max_delay < self.retry_initial_delay:
            raise InvalidConfigError(
                f"retry_max_delay ({self.retry_max_delay}) must be >= retry_initial_delay ({self.retry_initial_delay})"
            )
        if self.publish_timeout <= 0:
            raise InvalidConfigError(f"publish_timeout must be > 0, got {self.publish_timeout}")
        if self.reconnect_initial_delay <= 0:
            raise InvalidConfigError(f"reconnect_initial_delay must be > 0, got {self.reconnect_initial_delay}")
        if self.reconnect_max_delay < self.reconnect_initial_delay:
            raise InvalidConfigError(
                f"reconnect_max_delay ({self.reconnect_max_delay}) must be >= "
                f"reconnect_initial_delay ({self.reconnect_initial_delay})"
            )


def load_config(env: dict[str, str] | None = None) -> Config:
    """Lee la configuración del entorno y la valida.

    Un valor mal escrito no cae al default en silencio: un
    PUBSUB_PREFETCH=muchos ignorado sin avisar se descubre en producción como
    un consumo raro, no como un error de arranque.
    """
    source = os.environ if env is None else env

    exchange = _get(source, "RABBITMQ_EXCHANGE", DEFAULT_EXCHANGE)
    config = Config(
        url=_get(source, "RABBITMQ_URL", _DEFAULT_URL),
        exchange=exchange,
        dead_letter_exchange=_get(source, "RABBITMQ_DEAD_LETTER_EXCHANGE", exchange + _DEAD_LETTER_SUFFIX),
        service_name=_get(source, "SERVICE_NAME", ""),
        prefetch=_get_int(source, "PUBSUB_PREFETCH", DEFAULT_PREFETCH),
        max_retries=_get_int(source, "PUBSUB_MAX_RETRIES", DEFAULT_MAX_RETRIES),
        retry_initial_delay=_get_duration(source, "PUBSUB_RETRY_INITIAL_DELAY", DEFAULT_RETRY_INITIAL_DELAY),
        retry_max_delay=_get_duration(source, "PUBSUB_RETRY_MAX_DELAY", DEFAULT_RETRY_MAX_DELAY),
        publish_timeout=_get_duration(source, "PUBSUB_PUBLISH_TIMEOUT", DEFAULT_PUBLISH_TIMEOUT),
        reconnect_initial_delay=_get_duration(
            source, "PUBSUB_RECONNECT_INITIAL_DELAY", DEFAULT_RECONNECT_INITIAL_DELAY
        ),
        reconnect_max_delay=_get_duration(source, "PUBSUB_RECONNECT_MAX_DELAY", DEFAULT_RECONNECT_MAX_DELAY),
    )
    config.validate()
    return config


def _get(env, key: str, fallback: str) -> str:
    value = env.get(key, "")
    return value if value else fallback


def _get_int(env, key: str, fallback: int) -> int:
    raw = env.get(key, "")
    if not raw:
        return fallback
    try:
        return int(raw)
    except ValueError as exc:
        raise InvalidConfigError(f"{key}={raw!r} is not an integer") from exc


def _get_duration(env, key: str, fallback: float) -> float:
    raw = env.get(key, "")
    if not raw:
        return fallback
    return parse_duration(raw, key)


def parse_duration(raw: str, key: str = "duration") -> float:
    """Convierte una duración con el formato de Go (`200ms`, `5s`, `1m30s`).

    Se usa el formato de Go y no segundos pelados para que el mismo `.env`
    valga para los servicios de los dos lenguajes.
    """
    matches = _DURATION_PATTERN.findall(raw.strip())
    if not matches or "".join(number + unit for number, unit in matches) != raw.strip():
        raise InvalidConfigError(f"{key}={raw!r} is not a duration (use 200ms, 5s, 1m)")
    return sum(float(number) * _DURATION_UNITS[unit] for number, unit in matches)
