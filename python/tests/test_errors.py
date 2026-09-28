from discordia_pubsub.errors import PermanentError, is_permanent, permanent


class NotAMemberError(Exception):
    pass


def test_permanent_wraps_and_keeps_the_cause():
    wrapped = permanent(NotAMemberError("user is not a member"))

    assert is_permanent(wrapped)
    assert isinstance(wrapped.__cause__, NotAMemberError)
    assert "user is not a member" in str(wrapped)


def test_transient_by_default():
    assert not is_permanent(ConnectionResetError("connection reset by peer"))
    assert not is_permanent(None)


def test_survives_wrapping_with_context():
    try:
        try:
            raise PermanentError("channel does not belong to the server")
        except PermanentError as exc:
            raise RuntimeError("handling community.member.joined") from exc
    except RuntimeError as exc:
        outer = exc

    assert is_permanent(outer), "el handler puede envolver el error con contexto propio"


def test_survives_implicit_chaining():
    try:
        try:
            raise PermanentError("nope")
        except PermanentError:
            raise RuntimeError("boom")
    except RuntimeError as exc:
        outer = exc

    assert is_permanent(outer)


def test_handles_cyclic_cause_chains():
    first = RuntimeError("a")
    second = RuntimeError("b")
    first.__cause__ = second
    second.__cause__ = first

    assert not is_permanent(first), "una cadena de causas circular no puede colgar al consumer"
