---
name: discordia-adr-routing
description: Decide si una decisión de diseño en el cliente de Pub/Sub necesita un ADR, y si va en discordia-docs/adr (transversal) o en el adr/ de este repo, con qué numeración. Usar al evaluar una tecnología, un cambio de contrato o cualquier decisión que alguien podría preguntar "por qué se hizo así" dentro de un mes.
---

# ¿Esto necesita un ADR? ¿De cuál tipo?

Fuente: `discordia-docs/CONTRIBUTING.md`.

## Paso 1 — ¿Necesita un ADR?

Sí, si la respuesta a alguna de estas es "sí":

- ¿Cambia un contrato entre dos servicios (evento, envelope, topología)?
- ¿Elige una tecnología que después es cara de cambiar?
- ¿Contradice o justifica una restricción de la consigna?
- ¿Alguien del equipo va a preguntar "¿y por qué hicimos esto?" dentro de un
  mes?

No, si es una elección interna que se puede revertir sin avisarle a nadie
(una librería de tests, la estructura de archivos, cómo se nombran los
helpers).

**Ojo con este repo en particular:** casi todo lo que se cambia acá afecta a
los ocho servicios, así que el umbral para "esto necesita ADR" es más bajo
que en un repo de servicio.

## Paso 2 — ¿Global o de repo?

> Si la decisión la puede revertir un solo equipo sin romper a nadie, va en
> `adr/` de este repo. Si toca un contrato entre servicios o una restricción
> de la consigna, va en `discordia-docs/adr/`.

| Va en `discordia-docs/adr/` | Va en `adr/` de este repo |
|---|---|
| Forma del envelope, naming de eventos | Estructura de paquetes del cliente |
| Topología de colas y exchanges | Librería de AMQP elegida en cada lenguaje |
| Política de reintentos y dead-lettering | Estrategia de tests y de dobles |
| Tecnología del bus, proveedor gestionado | Formato de los logs internos |

Los defaults de `prefetch` y reintentos son la excepción: ADR-0003 los delegó
explícitamente al **README de este repo**, así que ahí van, no en un ADR
nuevo.

## Numeración

- Transversal: `ADR-0001`, `ADR-0002`, … secuencial, se reserva al abrir el
  PR, nunca se reusa un número.
- De este repo: `ADR-PUBSUB-0001`, `ADR-PUBSUB-0002`, …

## Ciclo de vida

`Propuesto` → `Aceptado` (fecha del merge) → `Reemplazado por ADR-00XX`, o
`Rechazado`. **Un ADR aceptado no se edita** — si la decisión cambia, se
escribe uno nuevo y al viejo se le pone `Reemplazado por ADR-00XX`.
Correcciones de typos o links sí se editan.
