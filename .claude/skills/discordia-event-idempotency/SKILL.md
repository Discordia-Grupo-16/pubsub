---
name: discordia-event-idempotency
description: Qué garantiza el cliente compartido de Pub/Sub y qué le queda al handler — entrega at-least-once, ack manual, reintentos con backoff, dead-letter queue y la idempotencia que el cliente no puede resolver. Usar al tocar el consumer o el ciclo de reintentos, y al explicarle a un servicio cómo tiene que escribir su handler.
---

# Idempotencia, orden y reintentos

Fuente: `discordia-docs/arquitectura/eventos.md`.

El bus da entrega **at-least-once**: todo handler puede recibir el mismo
evento más de una vez y tiene que tolerarlo. No es una recomendación, es de
cumplimiento obligatorio.

## Lo que resuelve el cliente

- Ack manual **después** de procesar, nunca al recibir.
- `prefetch` acotado por consumer (16 por defecto).
- Reintento en proceso con backoff exponencial **con jitter** ante un fallo
  transitorio.
- Dead-letter queue al agotar los reintentos, o inmediata si el fallo está
  marcado como permanente.
- Un mensaje que no es un envelope válido va a la DLQ sin llegar al handler.
- Reconexión al broker con backoff.

## Lo que le queda al handler

**La idempotencia.** El cliente no la puede resolver, porque "ya procesé
esto" significa algo distinto en cada dominio:

- **Inserts directos**: usar el `eventId` (o una clave de negocio) como
  restricción única y tratar el conflicto de duplicado como éxito, no como
  error.
- **Proyecciones locales**: la clave `eventId` no alcanza porque además hay
  que resolver el desorden. Cada entidad guarda un `lastEventAt`; un evento
  entrante se aplica **solo si su `occurredAt` es más nuevo**. Sin esto, un
  evento duplicado o reordenado puede pisar un estado más reciente con uno
  viejo.

**Clasificar el fallo.** Devolver `pubsub.Permanent(err)` en Go o levantar
`PermanentError` en Python cuando reintentar no puede cambiar el resultado.
Todo lo demás se trata como transitorio, que es el default seguro.

## Orden

El bus no garantiza orden global entre agregados distintos, y con cola
compartida tampoco entre eventos del mismo agregado. Ningún handler asume que
los eventos llegan en el orden en que ocurrieron: la única fuente de verdad
sobre "qué es más nuevo" es `occurredAt`, no el orden de llegada.

El consumo dentro de una cola es secuencial a propósito. Si aparece la idea
de paralelizarlo, tener presente que rompería del todo el poco orden que
queda; el paralelismo se consigue corriendo más instancias.

## Límite a tener presente

RabbitMQ no retiene historial una vez consumido. Si una proyección local se
corrompe o se pierde, no se puede reconstruir releyendo el bus: hace falta un
endpoint de re-sincronización en el servicio productor o recrearla desde un
seed.
