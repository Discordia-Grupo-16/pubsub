---
name: discordia-adr-routing
description: Decide si una decisión de diseño en discordia-chat necesita un ADR, y si va en discordia-docs/adr (transversal) o en el adr/ de este servicio, con qué numeración. Usar al evaluar una tecnología, un contrato entre servicios o cualquier decisión que alguien podría preguntar "por qué se hizo así" dentro de un mes.
---

# ¿Esto necesita un ADR? ¿De cuál tipo?

Fuente: `discordia-docs/CONTRIBUTING.md`.

## Paso 1 — ¿Necesita un ADR?

Sí, si la respuesta a alguna de estas es "sí":

- ¿Cambia un contrato entre dos servicios (evento, endpoint, esquema)?
- ¿Elige una tecnología que después es cara de cambiar (bus, DB, framework)?
- ¿Contradice o justifica una restricción de la consigna (comunicación
  sincrónica, lenguajes, tipo de DB)?
- ¿Alguien del equipo va a preguntar "¿y por qué hicimos esto?" dentro de un
  mes?

No, si es una elección interna de `chat` que se puede revertir sin avisarle a
nadie (una librería, la estructura de carpetas, cómo se ordenan los tests).

## Paso 2 — ¿Global o de servicio?

> Si la decisión la puede revertir un solo equipo sin romper a nadie, va en
> `<este-servicio>/adr/`. Si toca un contrato entre servicios o una
> restricción de la consigna, va en `discordia-docs/adr/`.

| Va en `discordia-docs/adr/` | Va en `adr/` de este repo |
|---|---|
| Tecnología del bus, formato de eventos | Librería de validación, driver de Mongo |
| Comunicaciones sincrónicas entre servicios | Estructura de `internal/` |
| Elección de lenguaje y motor de DB de `chat` | Estrategia de mocks en los tests |
| Proveedor cloud, CI/CD, secretos | Naming interno de paquetes |
| Autenticación y propagación de identidad | Formato de logs internos |

## Numeración

- Transversal: `ADR-0001`, `ADR-0002`, … secuencial, se reserva al abrir el
  PR, nunca se reusa un número.
- De este servicio: `ADR-CHAT-0001`, `ADR-CHAT-0002`, …

## Ciclo de vida

`Propuesto` → `Aceptado` (fecha del merge) → `Reemplazado por ADR-00XX`, o
`Rechazado`. **Un ADR aceptado no se edita** — si la decisión cambia, se
escribe uno nuevo y al viejo se le pone `Reemplazado por ADR-00XX`.
Correcciones de typos o links sí se editan.

Un ADR transversal se agrega al índice de `discordia-docs/adr/README.md` en
el mismo PR.
