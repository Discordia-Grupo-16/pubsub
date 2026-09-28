---
name: discordia-event-conventions
description: Convenciones de Discordia para los eventos del bus — envelope obligatorio, naming <servicio>.<agregado>.<evento-en-pasado>, versionado, UUID v4, fechas UTC e idioma del código. Usar al tocar el envelope, su validación, o al ayudar a un servicio a nombrar un evento nuevo.
---

# Convenciones de eventos de Discordia

Fuente: `discordia-docs/procesos/convenciones.md` y
`discordia-docs/arquitectura/eventos.md`. Este repo las **implementa**: la
validación del envelope es donde se hacen cumplir.

## Naming

- `<servicio>.<agregado>.<evento-en-pasado>`, ej. `chat.message.sent`. El
  verbo va en pasado: describe algo que ya ocurrió, no una orden.
- Son exactamente **tres segmentos** en minúscula. El cliente lo valida al
  construir el evento, porque el `eventType` es además la routing key: si no
  respeta esa forma, los bindings de los consumers no lo matchean y el evento
  no le llega a nadie.
- Cada servicio es dueño de su namespace: nadie publica en el de otro.

## Envelope

Campos obligatorios: `eventId`, `eventType`, `eventVersion`, `occurredAt`,
`correlationId`, `causationId` (opcional), `producer`, `data`.

- `data` es siempre un objeto JSON. Un payload escalar no se puede extender
  después sin romper a quien lo consume.
- `causationId` es el `eventId` del evento que provocó este; el
  `correlationId` se hereda. Es lo que mantiene trazable una cadena de
  reacciones asíncronas.

## Versionado

Un evento nuevo se agrega; **un evento existente no cambia de forma**. Si el
payload tiene que cambiar de manera incompatible, se publica `v2` en paralelo
y se deprecia el anterior.

Lo mismo vale para el envelope: cambiarle la forma rompe a los ocho servicios
a la vez. Si pasa, van los dos clientes y los fixtures de `testdata/` en el
mismo PR.

## Identificadores y fechas

- UUID v4 para todas las entidades de dominio.
- Fechas en UTC ISO-8601 con `Z`. Ojo con la precisión: Go serializa
  nanosegundos y Python solo lee hasta microsegundos, y por eso el cliente
  Python recorta. Hay un fixture con cada precisión en `testdata/`.

## Idioma

Código, nombres de variables, eventos, ramas y commits: **inglés**.
Documentación, comentarios y ADR: **español**.
