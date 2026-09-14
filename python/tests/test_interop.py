"""El contrato del sobre, escrito en un archivo.

Los fixtures de `testdata/` los emitió cada cliente y los leen los dos. Si
alguno cambia la forma del JSON, este test y su equivalente en Go
(interop_test.go) se rompen en el mismo PR.

El caso que más importa es occurredAt: Go serializa con precisión de
nanosegundos y Python solo llega a microsegundos.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from discordia_pubsub import Envelope

FIXTURES = Path(__file__).resolve().parents[2] / "testdata"


def read_fixture(name: str) -> Envelope:
    return Envelope.from_json((FIXTURES / name).read_text())


def test_reads_the_envelope_emitted_by_the_go_client():
    event = read_fixture("event-from-go.json")

    event.validate()
    assert event.event_type == "chat.message.sent"
    assert event.producer == "chat"
    assert event.causation_id == "c3a9f1d2-5e6b-4a70-8f21-9d4e7b0c5a38"
    assert event.data["content"] == "hola"
    # Go emite nanosegundos y datetime llega hasta microsegundos: se recorta,
    # no se rompe.
    assert event.occurred_at == datetime(2026, 9, 13, 20, 0, 0, 123456, tzinfo=timezone.utc)


def test_reads_the_envelope_emitted_by_the_python_client():
    event = read_fixture("event-from-python.json")

    event.validate()
    assert event.event_type == "identity.user.registered"
    assert event.producer == "identity"
    assert event.correlation_id == "8b2d0b7e-4f3a-4c1b-9a1e-2c5d7f0a3b64"
    assert event.data["email"] == "demo@discordia.test"
    assert event.occurred_at == datetime(2026, 9, 13, 20, 0, 0, 123456, tzinfo=timezone.utc)


def test_the_python_client_still_emits_the_fixture_shape():
    """Si esto falla, el cliente Python cambió la forma del JSON: hay que
    regenerar el fixture y actualizar el otro lado en el mismo PR."""
    event = read_fixture("event-from-python.json")

    assert event.to_dict() == json.loads((FIXTURES / "event-from-python.json").read_text())


def test_both_clients_agree_on_the_field_names():
    from_go = json.loads((FIXTURES / "event-from-go.json").read_text())
    from_python = json.loads((FIXTURES / "event-from-python.json").read_text())

    assert list(from_go) == list(from_python), "los dos clientes emiten los mismos campos, en el mismo orden"
