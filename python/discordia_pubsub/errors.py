"""Distinción entre fallos transitorios y permanentes.

Los RNF piden diferenciar fallos transitorios (reintentar) de permanentes
(compensar o notificar): un timeout de red se reintenta, un payload que viola
una regla de negocio va a fallar exactamente igual las tres veces siguientes
y solo ocupa la cola.

En Go el handler devuelve el error; acá lo levanta, que es lo idiomático en
Python. La semántica es la misma.
"""

from __future__ import annotations


class PermanentError(Exception):
    """Fallo del que no tiene sentido reintentar: va derecho a la DLQ.

        if not is_member(user_id):
            raise PermanentError(f"user {user_id} is not a member")

    O envolviendo la causa original:

        try:
            apply(event)
        except ValidationError as exc:
            raise PermanentError("payload does not meet the contract") from exc
    """


class ClosedError(RuntimeError):
    """El cliente ya se cerró y no acepta más operaciones."""


def permanent(exc: BaseException) -> PermanentError:
    """Envuelve una excepción como permanente, conservando la causa."""
    wrapped = PermanentError(str(exc))
    wrapped.__cause__ = exc
    return wrapped


def is_permanent(exc: BaseException | None) -> bool:
    """Responde si el fallo está marcado como permanente.

    Recorre la cadena de causas, así que un handler puede envolver el error
    con contexto propio sin perder la marca. Todo lo que no esté marcado se
    trata como transitorio, que es el default seguro.
    """
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, PermanentError):
            return True
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return False
