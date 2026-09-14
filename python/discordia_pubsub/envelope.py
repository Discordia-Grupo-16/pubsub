"""Sobre común de todo evento del bus, definido en INF-02.

Es el mismo contrato que implementa el cliente Go: los nombres de los campos
en el JSON son idénticos, así que un evento publicado por `identity` (Python)
lo consume `chat` (Go) sin traducción de por medio.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

# DEFAULT_EVENT_VERSION es la versión con la que nace todo evento nuevo. Un
# evento existente nunca cambia de forma: si el payload tiene que cambiar de
# manera incompatible se publica la versión siguiente en paralelo y se
# deprecia la anterior.
DEFAULT_EVENT_VERSION = 1

# El naming es `<servicio>.<agregado>.<evento-en-pasado>` (ej.
# `identity.user.registered`). Son exactamente tres segmentos en minúscula:
# los bindings de los consumers dependen de esa forma, así que un evento con
# otra forma no le llega a quien debería y conviene rechazarlo al construirlo.
_EVENT_TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*(?:\.[a-z][a-z0-9]*(?:-[a-z0-9]+)*){2}$")

# Go serializa los timestamps con precisión de nanosegundos y
# datetime.fromisoformat solo acepta hasta microsegundos. Recortar los
# dígitos de más es lo que hace que un evento publicado desde Go se pueda
# leer desde Python.
_TIMESTAMP_PATTERN = re.compile(r"^(.*?)(\.\d+)?(Z|[+-]\d{2}:?\d{2})$")


class InvalidEnvelopeError(ValueError):
    """El sobre no cumple el contrato de eventos."""


@dataclass(frozen=True, slots=True)
class Envelope:
    """Un evento del bus.

    Es inmutable a propósito: un evento describe algo que ya ocurrió, y un
    handler que lo modificara estaría cambiando el pasado para los demás
    handlers de su mismo proceso.
    """

    event_id: str
    event_type: str
    event_version: int
    occurred_at: datetime
    correlation_id: str
    producer: str
    data: dict[str, Any]
    causation_id: str | None = None

    @property
    def routing_key(self) -> str:
        """Clave con la que el evento se publica en el topic exchange.

        Coincide con el event_type por decisión de ADR-0003: así el binding
        de cada consumer se lee igual que el nombre del evento.
        """
        return self.event_type

    def validate(self) -> None:
        """Verifica que el sobre cumpla el contrato de INF-02."""
        _require_uuid(self.event_id, "eventId")
        if not _EVENT_TYPE_PATTERN.match(self.event_type or ""):
            raise InvalidEnvelopeError(
                f"eventType {self.event_type!r} must be <service>.<aggregate>.<past-tense-event>"
            )
        if not isinstance(self.event_version, int) or isinstance(self.event_version, bool) or self.event_version < 1:
            raise InvalidEnvelopeError(f"eventVersion must be an integer >= 1, got {self.event_version!r}")
        if not isinstance(self.occurred_at, datetime) or self.occurred_at.tzinfo is None:
            raise InvalidEnvelopeError("occurredAt must be a timezone-aware datetime")
        _require_uuid(self.correlation_id, "correlationId")
        if self.causation_id is not None:
            _require_uuid(self.causation_id, "causationId")
        if not self.producer:
            raise InvalidEnvelopeError("producer is required")
        if not isinstance(self.data, dict):
            raise InvalidEnvelopeError("data must be a JSON object")

    def to_dict(self) -> dict[str, Any]:
        """Serializa el sobre con las claves en camelCase del contrato."""
        payload: dict[str, Any] = {
            "eventId": self.event_id,
            "eventType": self.event_type,
            "eventVersion": self.event_version,
            "occurredAt": _format_timestamp(self.occurred_at),
            "correlationId": self.correlation_id,
        }
        # Mismo orden de claves que el cliente Go: los dos lados emiten un
        # JSON que se lee igual, y los fixtures de interoperabilidad se
        # comparan de un vistazo.
        if self.causation_id:
            payload["causationId"] = self.causation_id
        payload["producer"] = self.producer
        payload["data"] = self.data
        return payload

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict()).encode()

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Envelope:
        if not isinstance(payload, dict):
            raise InvalidEnvelopeError("an event must be a JSON object")
        try:
            return cls(
                event_id=payload["eventId"],
                event_type=payload["eventType"],
                event_version=payload["eventVersion"],
                occurred_at=_parse_timestamp(payload["occurredAt"]),
                correlation_id=payload["correlationId"],
                producer=payload["producer"],
                data=payload["data"],
                causation_id=payload.get("causationId"),
            )
        except KeyError as exc:
            raise InvalidEnvelopeError(f"missing field {exc.args[0]!r}") from exc

    @classmethod
    def from_json(cls, raw: bytes | str) -> Envelope:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise InvalidEnvelopeError(f"event is not valid JSON: {exc}") from exc
        return cls.from_dict(payload)


def new_envelope(
    event_type: str,
    producer: str,
    data: dict[str, Any],
    *,
    correlation_id: str | None = None,
    cause: Envelope | None = None,
    event_version: int = DEFAULT_EVENT_VERSION,
    occurred_at: datetime | None = None,
) -> Envelope:
    """Arma un evento listo para publicar y lo valida.

    `cause` marca este evento como consecuencia de otro: hereda su
    correlation ID y guarda su event ID como causation ID. Es la forma de que
    una cadena de reacciones asíncronas siga siendo trazable.
    """
    if cause is not None:
        correlation_id = correlation_id or cause.correlation_id

    envelope = Envelope(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        event_version=event_version,
        occurred_at=(occurred_at or datetime.now(timezone.utc)).astimezone(timezone.utc),
        correlation_id=correlation_id or str(uuid.uuid4()),
        producer=producer,
        data=data,
        causation_id=cause.event_id if cause is not None else None,
    )
    envelope.validate()
    return envelope


def with_data(envelope: Envelope, data: dict[str, Any]) -> Envelope:
    """Devuelve una copia del sobre con otro payload, sin tocar el original."""
    return replace(envelope, data=data)


def _require_uuid(value: Any, field: str) -> None:
    try:
        uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError) as exc:
        raise InvalidEnvelopeError(f"{field} must be a UUID, got {value!r}") from exc


def _format_timestamp(value: datetime) -> str:
    """Fechas en UTC ISO-8601 con Z, como las emite el cliente Go."""
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw
    if not isinstance(raw, str):
        raise InvalidEnvelopeError(f"occurredAt must be an ISO-8601 string, got {raw!r}")

    match = _TIMESTAMP_PATTERN.match(raw)
    if match is None:
        raise InvalidEnvelopeError(f"occurredAt {raw!r} is not an ISO-8601 timestamp")

    head, fraction, offset = match.groups()
    # Go emite hasta nanosegundos; datetime solo entiende microsegundos.
    fraction = (fraction or "")[:7]
    normalized = f"{head}{fraction}{'+00:00' if offset == 'Z' else offset}"

    try:
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    except ValueError as exc:
        raise InvalidEnvelopeError(f"occurredAt {raw!r} is not an ISO-8601 timestamp") from exc
