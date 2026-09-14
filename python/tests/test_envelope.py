import dataclasses
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from discordia_pubsub import Envelope, InvalidEnvelopeError, new_envelope, with_data


def test_new_envelope_fills_defaults():
    before = datetime.now(timezone.utc)

    envelope = new_envelope("identity.user.registered", "identity", {"userId": "u1"})

    assert uuid.UUID(envelope.event_id)
    assert envelope.event_id != envelope.correlation_id
    assert envelope.event_version == 1
    assert envelope.causation_id is None
    assert envelope.occurred_at.tzinfo is timezone.utc
    assert envelope.occurred_at >= before


def test_routing_key_is_the_event_type():
    envelope = new_envelope("community.member.joined", "community", {"userId": "u1"})

    assert envelope.routing_key == "community.member.joined"


def test_new_envelope_accepts_an_explicit_correlation_id():
    correlation_id = str(uuid.uuid4())

    envelope = new_envelope("identity.user.registered", "identity", {}, correlation_id=correlation_id)

    assert envelope.correlation_id == correlation_id


def test_cause_keeps_the_trace():
    parent = new_envelope("mod.member.banned", "mod", {"userId": "u1"})

    child = new_envelope("identity.session.revoked", "identity", {"userId": "u1"}, cause=parent)

    assert child.correlation_id == parent.correlation_id
    assert child.causation_id == parent.event_id
    assert child.event_id != parent.event_id


def test_occurred_at_is_normalized_to_utc():
    art = timezone(timedelta(hours=-3))
    occurred = datetime(2026, 9, 13, 20, 0, tzinfo=art)

    envelope = new_envelope("metrics.rollup.computed", "metrics", {}, occurred_at=occurred)

    assert envelope.occurred_at == occurred.astimezone(timezone.utc)
    assert envelope.occurred_at.tzinfo is timezone.utc


@pytest.mark.parametrize(
    "event_type",
    ["identity.registered", "identity.user.account.registered", "Identity.User.Registered", "", "identity..registered"],
)
def test_new_envelope_rejects_malformed_event_types(event_type):
    with pytest.raises(InvalidEnvelopeError):
        new_envelope(event_type, "identity", {})


def test_new_envelope_accepts_hyphenated_service_names():
    envelope = new_envelope("chat-and-real-time.voice-channel.joined", "chat-and-real-time", {})

    assert envelope.event_type == "chat-and-real-time.voice-channel.joined"


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_id", "no-uuid"),
        ("correlation_id", "no-uuid"),
        ("causation_id", "no-uuid"),
        ("event_version", 0),
        ("event_version", True),
        ("producer", ""),
        ("data", "no soy un objeto"),
        ("occurred_at", datetime(2026, 9, 13, 20, 0)),
    ],
)
def test_validate_rejects_broken_fields(field, value):
    valid = new_envelope("identity.user.registered", "identity", {"userId": "u1"})

    broken = dataclasses.replace(valid, **{field: value})

    with pytest.raises(InvalidEnvelopeError):
        broken.validate()


def test_json_uses_camel_case_and_utc():
    envelope = new_envelope(
        "identity.user.registered",
        "identity",
        {"userId": "u1"},
        occurred_at=datetime(2026, 9, 13, 20, 0, tzinfo=timezone.utc),
    )

    payload = json.loads(envelope.to_json())

    assert set(payload) == {"eventId", "eventType", "eventVersion", "occurredAt", "correlationId", "producer", "data"}
    assert payload["occurredAt"] == "2026-09-13T20:00:00Z"


def test_causation_id_is_omitted_when_absent_and_present_when_set():
    parent = new_envelope("mod.member.banned", "mod", {})
    child = new_envelope("identity.session.revoked", "identity", {}, cause=parent)

    assert "causationId" not in json.loads(parent.to_json())
    assert json.loads(child.to_json())["causationId"] == parent.event_id


def test_round_trip():
    original = new_envelope("identity.user.registered", "identity", {"userId": "u1"})

    decoded = Envelope.from_json(original.to_json())
    decoded.validate()

    assert decoded == original


# El cliente Go serializa con precisión de nanosegundos y fromisoformat solo
# acepta microsegundos: sin recortar, un evento publicado desde Go no se
# podría leer desde Python.
@pytest.mark.parametrize(
    "raw,expected_microsecond",
    [
        ("2026-09-13T20:00:00Z", 0),
        ("2026-09-13T20:00:00.123Z", 123000),
        ("2026-09-13T20:00:00.123456Z", 123456),
        ("2026-09-13T20:00:00.123456789Z", 123456),
        ("2026-09-13T17:00:00-03:00", 0),
    ],
)
def test_parses_timestamps_from_the_go_client(raw, expected_microsecond):
    envelope = Envelope.from_dict(
        {
            "eventId": str(uuid.uuid4()),
            "eventType": "chat.message.sent",
            "eventVersion": 1,
            "occurredAt": raw,
            "correlationId": str(uuid.uuid4()),
            "producer": "chat",
            "data": {"messageId": "m1"},
        }
    )

    envelope.validate()
    assert envelope.occurred_at.tzinfo is timezone.utc
    assert envelope.occurred_at.microsecond == expected_microsecond
    assert envelope.occurred_at.hour == 20


@pytest.mark.parametrize("raw", ["ayer", 12345, "2026-09-13T20:00:00"])
def test_rejects_unparseable_timestamps(raw):
    with pytest.raises(InvalidEnvelopeError):
        Envelope.from_dict(
            {
                "eventId": str(uuid.uuid4()),
                "eventType": "chat.message.sent",
                "eventVersion": 1,
                "occurredAt": raw,
                "correlationId": str(uuid.uuid4()),
                "producer": "chat",
                "data": {},
            }
        )


def test_from_json_rejects_garbage():
    with pytest.raises(InvalidEnvelopeError):
        Envelope.from_json(b"no soy json")

    with pytest.raises(InvalidEnvelopeError):
        Envelope.from_json(b"[1, 2, 3]")


def test_from_dict_reports_the_missing_field():
    with pytest.raises(InvalidEnvelopeError, match="eventType"):
        Envelope.from_dict({"eventId": str(uuid.uuid4())})


def test_envelope_is_immutable():
    envelope = new_envelope("identity.user.registered", "identity", {"userId": "u1"})

    with pytest.raises(Exception):
        envelope.event_type = "otra.cosa.distinta"

    changed = with_data(envelope, {"userId": "u2"})
    assert envelope.data == {"userId": "u1"}
    assert changed.data == {"userId": "u2"}
