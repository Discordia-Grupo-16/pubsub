"""Cliente de Pub/Sub sobre RabbitMQ."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

import aio_pika

from discordia_pubsub.config import Config, load_config
from discordia_pubsub.envelope import Envelope, InvalidEnvelopeError
from discordia_pubsub.errors import ClosedError, is_permanent
from discordia_pubsub.retry import Handler, RetryPolicy, run_with_retries
from discordia_pubsub.topology import Subscription, declare_exchanges, declare_subscription

ConnectionFactory = Callable[..., Awaitable[Any]]

_logger = logging.getLogger("discordia_pubsub")


class Client:
    """Conexión de un servicio al bus. Una por proceso.

    A diferencia del cliente Go, la reconexión no la maneja este código: se
    usa `aio_pika.connect_robust`, que ya rehace la conexión, los canales,
    las colas y los consumers cuando el broker vuelve. Escribir un
    supervisor propio al lado sería duplicar —y contradecir— lo que la
    librería ya hace bien.
    """

    def __init__(
        self,
        config: Config,
        connection: Any,
        channel: Any,
        exchange: Any,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._connection = connection
        self._channel = channel
        self._exchange = exchange
        self._logger = logger or _logger
        self._policy = RetryPolicy.from_config(config)
        self._closed = False

    @classmethod
    async def connect(
        cls,
        config: Config | None = None,
        *,
        logger: logging.Logger | None = None,
        connection_factory: ConnectionFactory = aio_pika.connect_robust,
    ) -> Client:
        """Abre la conexión, declara los exchanges y deja el cliente listo.

        Falla si el broker no está disponible: un servicio que arranca sin
        bus es un servicio que va a fallar en la primera operación.
        """
        config = config or load_config()
        config.validate()

        connection = await connection_factory(
            config.url,
            # El nombre de la conexión es lo que hace legible la management
            # UI cuando hay ocho servicios colgados del mismo broker.
            client_properties={"connection_name": config.service_name},
        )
        # publisher_confirms: sin confirms, publish vuelve apenas escribe en
        # el socket y un evento que el broker rechaza se pierde sin que nadie
        # se entere.
        channel = await connection.channel(publisher_confirms=True)
        exchange = await declare_exchanges(channel, config)

        return cls(config, connection, channel, exchange, logger)

    @property
    def config(self) -> Config:
        return self._config

    async def publish(self, event: Envelope) -> None:
        """Manda un evento al exchange usando su eventType como routing key.

        No usa el flag mandatory: que todavía no haya ningún consumer atado a
        ese evento es normal en pub/sub y no es un error del publicador.
        """
        event.validate()
        if self._closed:
            raise ClosedError("pubsub client is closed")

        message = aio_pika.Message(
            body=event.to_json(),
            message_id=event.event_id,
            correlation_id=event.correlation_id,
            type=event.event_type,
            app_id=event.producer,
            timestamp=event.occurred_at,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await self._exchange.publish(
            message,
            routing_key=event.routing_key,
            mandatory=False,
            timeout=self._config.publish_timeout,
        )

        self._logger.debug(
            "event published",
            extra={"eventType": event.event_type, "eventId": event.event_id, "correlationId": event.correlation_id},
        )

    async def subscribe(self, subscription: Subscription, handler: Handler) -> Any:
        """Registra un consumer y lo deja corriendo. Devuelve la cola.

        La subscripción vive hasta que se cierra el cliente y se rehace sola
        si el broker se cae.
        """
        subscription.validate()
        if handler is None:
            raise ValueError("handler is required")
        if self._closed:
            raise ClosedError("pubsub client is closed")

        # Cada subscripción usa su propio canal: el prefetch se configura por
        # canal, así que compartirlo mezclaría el límite de dos consumers con
        # ritmos distintos.
        channel = await self._connection.channel()
        queue = await declare_subscription(channel, self._config, subscription)

        # no_ack en False: el ack va después de procesar. Con no_ack, un
        # evento se da por procesado apenas se entrega y se pierde si el
        # proceso se cae a mitad de camino.
        await queue.consume(
            self._callback(subscription, handler),
            no_ack=False,
            exclusive=subscription.queue.per_instance,
        )

        self._logger.info(
            "subscribed",
            extra={
                "queue": queue.name,
                "mode": str(subscription.queue),
                "bindingKeys": list(subscription.binding_keys),
                "prefetch": subscription.effective_prefetch(self._config),
            },
        )
        return queue

    def _callback(self, subscription: Subscription, handler: Handler):
        """Arma el callback que aio-pika llama por cada mensaje."""

        async def on_message(message: Any) -> None:
            try:
                event = Envelope.from_json(message.body)
                event.validate()
            except InvalidEnvelopeError as exc:
                # Un mensaje que no cumple el contrato no se vuelve válido
                # por insistir, y mientras tanto tapa la cola.
                self._logger.error(
                    "message dead-lettered",
                    extra={"reason": "invalid envelope", "messageId": message.message_id, "error": str(exc)},
                )
                await message.nack(requeue=False)
                return

            try:
                await run_with_retries(handler, event, self._policy)
            except asyncio.CancelledError:
                # Nunca se llegó a procesar: vuelve a la cola para que lo tome
                # otra instancia o esta misma después de reiniciar.
                await message.nack(requeue=True)
                raise
            except Exception as exc:
                self._logger.error(
                    "event dead-lettered",
                    extra={
                        "eventType": event.event_type,
                        "eventId": event.event_id,
                        "correlationId": event.correlation_id,
                        "permanent": is_permanent(exc),
                        "error": str(exc),
                    },
                )
                await message.nack(requeue=False)
                return

            await message.ack()

        return on_message

    async def close(self) -> None:
        """Cierra la conexión y con ella los canales y las subscripciones."""
        if self._closed:
            return
        self._closed = True
        await self._connection.close()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


async def connect(
    config: Config | None = None,
    *,
    logger: logging.Logger | None = None,
) -> Client:
    """Atajo de Client.connect."""
    return await Client.connect(config, logger=logger)
