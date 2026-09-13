---
name: discordia-event-idempotency
description: Reglas de idempotencia, orden y reintentos para consumers de eventos en discordia-chat, dado que RabbitMQ entrega at-least-once. Usar al escribir o revisar un handler de evento en internal/bus, sobre todo si hace un insert o mantiene una proyección local alimentada por eventos de otro servicio (ej. autorización de community).
---

# Idempotencia, orden y reintentos de eventos

Fuente: `discordia-docs/arquitectura/eventos.md`. El bus da entrega
**at-least-once**: todo handler puede recibir el mismo evento más de una vez
y tiene que tolerarlo. No es una recomendación, es de cumplimiento
obligatorio.

## Idempotencia

- Todo handler es idempotente: procesar el mismo `eventId` dos veces produce
  el mismo resultado que procesarlo una vez.
- **Inserts directos**: usar el `eventId` (o una clave de negocio) como
  restricción única en Mongo y tratar el conflicto de duplicado como éxito,
  no como error.
- **Proyecciones locales** (ej. la proyección de autorización que `chat`
  mantiene a partir de eventos de `community`): la clave `eventId` no alcanza
  porque además hay que resolver el desorden. Cada entidad de la proyección
  guarda un `lastEventAt`; un evento entrante se aplica **solo si su
  `occurredAt` es más nuevo que el `lastEventAt` guardado**. Sin esto, un
  evento duplicado o reordenado puede pisar un estado más reciente con uno
  viejo.

## Orden

- El bus no garantiza orden global entre agregados distintos, y con cola
  compartida tampoco lo garantiza entre eventos del mismo agregado (ver
  `discordia-rabbitmq-topology`).
- Ningún handler asume que los eventos llegan en el orden en que ocurrieron.
  La única fuente de verdad sobre "qué es más nuevo" es `lastEventAt`, no el
  orden de llegada.

## Reintentos y dead-letter

- Ack manual **después** de procesar el evento con éxito, nunca al recibirlo
  — si el proceso se cae a mitad de camino, el evento no se pierde, vuelve a
  la cola.
- `prefetch` acotado por consumer, para no acumular en memoria más de lo que
  se puede procesar.
- Reintento con backoff exponencial dentro del proceso ante un error
  transitorio, antes de dar el evento por fallido.
- Agotados los reintentos, el evento va a una dead-letter queue
  (`x-dead-letter-exchange`) en vez de perderse o bloquear la cola principal.
  Un evento en la DLQ requiere intervención manual, no hay reproceso
  automático.

## Límite a tener presente

RabbitMQ no retiene historial una vez consumido. Si una proyección local se
corrompe o se pierde, no se puede reconstruir releyendo el bus — hace falta
un endpoint de re-sincronización en el servicio productor o recrearla desde
un seed.
