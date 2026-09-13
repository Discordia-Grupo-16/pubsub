---
name: discordia-api-event-conventions
description: Convenciones de naming y formato de Discordia para endpoints HTTP y eventos nuevos en discordia-chat — recursos en plural, camelCase, forma de error estándar, UUID v4, fechas UTC, naming de eventos en pasado. Usar al agregar un endpoint en internal/transport o un evento nuevo en internal/bus.
---

# Convenciones de HTTP y eventos de Discordia

Fuente: `discordia-docs/procesos/convenciones.md` y
`discordia-docs/arquitectura/eventos.md`.

## HTTP

- Recursos en **plural**: `/servers`, `/servers/{serverId}/channels`. Sin
  verbos en la URL — la acción la da el método HTTP.
- `camelCase` en los JSON (el front es JavaScript en los tres artefactos).
- Errores con la misma forma en todos los servicios:

  ```json
  { "error": { "code": "SERVER_NOT_FOUND", "message": "...", "correlationId": "uuid" } }
  ```

## Eventos

- Naming: `<servicio>.<agregado>.<evento-en-pasado>`, ej. `chat.message.sent`.
  El verbo va en pasado: describe algo que ya ocurrió, no una orden.
- `chat` es dueño del namespace `chat.*` — nadie más publica ahí, y `chat` no
  publica en el namespace de otro servicio.
- Un evento nuevo se agrega; **un evento existente no cambia de forma**. Si
  el payload tiene que cambiar de manera incompatible, se publica `v2` en
  paralelo y se deprecia el anterior.
- Envelope común obligatorio: `eventId`, `eventType`, `eventVersion`,
  `occurredAt`, `correlationId`, `causationId`, `producer`, `data`.
- Todo evento nuevo que publica o consume `chat` se agrega a
  `discordia-docs/arquitectura/eventos.md` en el mismo PR (ver
  `discordia-dod-checklist`).

## Identificadores y fechas

- UUID v4 para todas las entidades de dominio. Nunca exponer ids
  autoincrementales en la API.
- Fechas en UTC ISO-8601 con `Z` (`2026-09-08T14:32:00Z`), tanto en la API
  como en los eventos. La conversión a hora local es responsabilidad del
  front.

## Idioma

Código, nombres de variables, eventos, endpoints, ramas y commits: **inglés**.
Documentación y ADR: **español**.
