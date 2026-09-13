---
name: discordia-rabbitmq-topology
description: Cómo declarar colas de RabbitMQ para discordia-chat sobre el exchange topic `discordia.events` — cola compartida vs. cola exclusive/auto-delete por instancia. Usar al escribir o revisar código en internal/bus que declara una cola, un binding o un consumer nuevo.
---

# Topología de colas de `chat` en RabbitMQ

Fuente: `discordia-docs/arquitectura/eventos.md` (sección "Topología de colas").

Un único exchange **topic**, durable: `discordia.events`. La routing key es el
`eventType` completo (`community.member.joined`). Cada consumer elige su
patrón de binding.

Sobre ese exchange hay **dos semánticas de cola, no intercambiables**:

| Semántica | Cómo se declara | Para qué |
|---|---|---|
| Cola compartida | Durable, nombre fijo (`chat.community-projection`) | Las N instancias compiten por la cola: cada evento lo procesa **una sola**. Usar para todo lo que termina escribiendo en el Mongo de `chat` |
| Cola por instancia | `exclusive` + `auto-delete`, nombre generado por el broker | Cada instancia recibe **todos** los eventos. Es el único modo que hace funcionar el fan-out multiinstancia |

`chat` necesita las dos, para cosas distintas:

- **Cola compartida** para consumir los eventos de `community`
  (`community.channel.created/updated/deleted`, `community.member.joined/left`)
  que alimentan la proyección local de autorización — el Mongo de `chat` es
  uno solo, no tiene sentido que tres instancias apliquen el mismo evento tres
  veces.
- **Cola por instancia** para `chat.message.sent` — cada instancia tiene un
  conjunto distinto de clientes WebSocket conectados (`internal/realtime`), así
  que cada una necesita enterarse de **todos** los mensajes para reenviárselos
  a los suyos.

## Riesgo a no repetir

Si `chat.message.sent` se declara por error como cola compartida, el mensaje
le llega a una sola instancia — es decir, a una fracción de los usuarios
conectados. **No tira error**, solo hay clientes que nunca reciben nada, y es
muy difícil de diagnosticar sin saber que la topología estaba mal desde el
arranque. Antes de mergear un consumer nuevo en `internal/bus`, confirmar
explícitamente cuál de las dos semánticas le corresponde y por qué.
