---
name: discordia-rabbitmq-topology
description: Cómo se declaran las colas de RabbitMQ en el cliente compartido de Discordia — cola compartida vs. cola exclusive/auto-delete por instancia, y por qué la elección no puede tener default. Usar al tocar topology.go o topology.py, al agregar una opción de subscripción, o al ayudar a un servicio a elegir con qué semántica consumir un evento.
---

# Topología de colas del cliente compartido

Fuente: [ADR-0003](https://github.com/Discordia-Grupo-16/discordia-docs/blob/dev/adr/0003-tecnologia-del-bus-pubsub.md)
y `discordia-docs/arquitectura/eventos.md`.

Un único exchange **topic**, durable: `discordia.events`. La routing key es
el `eventType` completo (`community.member.joined`). Cada consumer elige su
patrón de binding.

Sobre ese exchange hay **dos semánticas de cola, no intercambiables**:

| Semántica | Cómo se declara | Para qué |
|---|---|---|
| Cola compartida | Durable, nombre fijo, con DLQ propia | Las N réplicas compiten por la cola: cada evento lo procesa **una sola**. Para todo lo que termina escribiendo en la base del servicio |
| Cola por instancia | `exclusive` + `auto-delete`, nombre generado por el broker, sin DLQ | Cada instancia recibe **todos** los eventos. Es el único modo que hace funcionar el fan-out multiinstancia |

Se eligen con `SharedQueue(nombre)` / `PerInstanceQueue()` en Go y
`shared_queue(nombre)` / `per_instance_queue()` en Python.

## Reglas de este repo

- **La elección nunca tiene default.** `QueueSpec` no se puede construir
  vacío y `Subscription` la pide sin valor por defecto. Si alguna vez alguien
  propone "que por defecto sea compartida", la respuesta es no: el default
  silencioso es exactamente el bug que esto previene.
- **Una cola por instancia no lleva dead-lettering.** Acumular eventos
  muertos de instancias que ya no existen es basura, no resiliencia, y lo que
  se pierde ahí se recupera del historial persistido.
- **Los dos clientes declaran lo mismo.** Cualquier cambio en `topology.go`
  va también en `topology.py`, en el mismo PR, con su test de cada lado.

## El riesgo a no repetir

Si el fan-out de mensajería se declara como cola compartida, el mensaje le
llega a una sola instancia — es decir, a una fracción de los usuarios
conectados. **No tira error**, solo hay clientes que nunca reciben nada, y es
muy difícil de diagnosticar sin saber que la topología estaba mal desde el
arranque.

Por eso hay un test en cada lenguaje que verifica que las dos declaraciones
difieren en `exclusive`, `auto-delete` y `durable`, y otro de integración que
prueba el fan-out con dos clientes reales. Si tocás la declaración de colas,
esos tests tienen que seguir en verde sin aflojar la aserción.
