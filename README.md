# discordia-pubsub

Cliente compartido de Pub/Sub sobre RabbitMQ para los servicios de Discordia
(**INF-06**). Una sola implementación de publish/subscribe, en Go y en
Python, que todos los servicios usan en vez de hablarle a RabbitMQ por su
cuenta.

> Este repositorio es una **librería**, no un servicio: no expone HTTP, no
> tiene base de datos y no se despliega solo.

## Por qué existe

[ADR-0003](https://github.com/Discordia-Grupo-16/discordia-docs/blob/dev/adr/0003-tecnologia-del-bus-pubsub.md)
eligió RabbitMQ y dejó **dos topologías distintas sobre el mismo broker**:
colas compartidas para eventos de dominio y colas por instancia para el
fan-out de mensajería. Confundirlas **no tira error**: el mensaje le llega a
una sola instancia y los clientes conectados a las demás no ven nada.

Este cliente encapsula esa diferencia detrás de una API única, y concentra
—una vez y para los ocho servicios— el envelope de INF-02, el ack manual, el
`prefetch`, el backoff, la dead-letter queue y la reconexión.

| | |
| --- | --- |
| Go | `import "github.com/Discordia-Grupo-16/pubsub"` |
| Python | paquete `discordia_pubsub`, en [`python/`](python/) |

Los dos clientes tienen la misma API, las mismas variables de entorno y el
mismo formato de evento. [`testdata/`](testdata/) tiene un sobre emitido por
cada uno, y las dos suites lo leen: si un lado cambia la forma del JSON, se
rompen los tests de los dos en el mismo PR.

## Instalación

**Go**

```bash
go get github.com/Discordia-Grupo-16/pubsub
```

**Python**

```bash
pip install "git+https://github.com/Discordia-Grupo-16/pubsub@dev#subdirectory=python"
```

## Cómo se usa

### Publicar

```go
cfg, err := pubsub.LoadConfig()
client, err := pubsub.Connect(ctx, cfg)
defer client.Close()

event, err := pubsub.NewEnvelope("chat.message.sent", "chat", MessageSent{
    MessageID: id, ChannelID: channelID,
}, pubsub.WithCorrelationID(correlationID))

err = client.Publish(ctx, event)
```

```python
client = await Client.connect()

event = new_envelope(
    "identity.user.registered", "identity",
    {"userId": user_id, "email": email},
    correlation_id=correlation_id,
)
await client.publish(event)
```

El `correlationId` viene del api-gateway y se propaga tal cual. Si el evento
es **consecuencia de otro**, se encadena con `WithCause(parent)` / `cause=`:
hereda el correlation ID y guarda el `eventId` del padre como `causationId`.
Eso es lo que mantiene trazable una cadena de reacciones asíncronas.

### Consumir

```go
err := client.Subscribe(ctx, pubsub.Subscription{
    Queue:       pubsub.SharedQueue("chat.community-projection"),
    BindingKeys: []string{"community.channel.*", "community.member.joined"},
}, func(ctx context.Context, event pubsub.Envelope) error {
    var payload MemberJoined
    if err := event.UnmarshalData(&payload); err != nil {
        return pubsub.Permanent(err)
    }
    return projection.Apply(ctx, event, payload)
})
```

```python
await client.subscribe(
    Subscription(
        queue=shared_queue("community.identity-projection"),
        binding_keys=["identity.user.registered"],
    ),
    projection.apply,
)
```

`Subscribe` no bloquea: registra el consumer y sigue. La subscripción vive
hasta que se cancela el contexto o se cierra el cliente.

### Elegir la semántica de cola

Es la única decisión que el cliente **no** puede tomar por vos, y por eso no
tiene valor por defecto:

| | `SharedQueue(nombre)` / `shared_queue(nombre)` | `PerInstanceQueue()` / `per_instance_queue()` |
| --- | --- | --- |
| Quién procesa cada evento | **Una** réplica del servicio | **Todas** las instancias |
| Cola | Durable, nombre fijo | `exclusive` + `auto-delete`, nombre del broker |
| Sobrevive al reinicio | Sí | No |
| Dead-letter queue | Sí, propia | No |
| Cuándo usarla | El handler escribe en la base del servicio: proyecciones, contadores, auditoría | El handler depende del estado local de *esa* instancia: reenviar a los WebSockets conectados |

> **El riesgo a no repetir.** Si el fan-out de mensajería se declara como
> cola compartida, el mensaje le llega a una sola instancia y los clientes de
> las demás no reciben nada. No hay error, no hay log: solo usuarios que no
> ven los mensajes. Antes de mergear un consumer nuevo, decí en voz alta cuál
> de las dos le corresponde y por qué.

### Fallos: transitorio vs. permanente

Los RNF piden diferenciar **fallos transitorios** (reintentar) de
**permanentes** (compensar o notificar):

| El handler… | Go | Python | Qué pasa |
| --- | --- | --- | --- |
| Terminó bien | `return nil` | termina sin excepción | `ack`, sale de la cola |
| Falló por algo pasajero | `return err` | levanta cualquier excepción | Reintento con backoff; agotados, a la DLQ |
| Falló por algo definitivo | `return pubsub.Permanent(err)` | levanta `PermanentError` | A la DLQ, sin reintentar |

Un mensaje que no es un envelope válido va a la DLQ sin llegar nunca al
handler: no se vuelve válido por insistir y mientras tanto tapa la cola.

### Idempotencia: es tuya, no del cliente

El bus entrega **at-least-once**. Todo handler puede recibir el mismo evento
más de una vez y tiene que tolerarlo. El cliente no lo puede resolver por
vos, porque "ya procesé esto" significa algo distinto en cada dominio:

- **Inserts:** usá el `eventId` o una clave de negocio como restricción única
  y tratá el duplicado como éxito, no como error.
- **Proyecciones:** guardá un `lastEventAt` por entidad y aplicá el evento
  solo si su `occurredAt` es más nuevo. El bus tampoco garantiza orden.

## Testear un handler sin levantar RabbitMQ

Los dos clientes traen un bus en memoria con la misma API y la misma
semántica de reparto. Tipá contra la interfaz (`pubsub.Bus` / el `Protocol`
`Bus`) y en los tests entra el fake:

```go
bus := pubsubtest.New()
require.NoError(t, bus.Subscribe(ctx, spec, projection.Handle))
require.NoError(t, bus.Publish(ctx, event))
// acá el handler ya corrió: la entrega es sincrónica
assert.Empty(t, bus.DeadLettered())
```

```python
bus = InMemoryBus()
await bus.subscribe(subscription, projection.apply)
await bus.publish(event)
assert bus.dead_lettered == []
```

Reproduce las dos semánticas de cola, así que se puede verificar el fan-out y
el round-robin sin broker.

## Configuración

Las mismas variables en los dos lenguajes: un `.env` sirve para un servicio
Python y para uno Go. Ver [`.env.example`](.env.example).

| Variable | Default | Para qué |
| --- | --- | --- |
| `RABBITMQ_URL` | `amqp://localhost:5672/` | Conexión al broker, credenciales incluidas |
| `SERVICE_NAME` | *(obligatoria)* | Nombra la conexión en la management UI y prefija las colas |
| `RABBITMQ_EXCHANGE` | `discordia.events` | Topic exchange de eventos de dominio |
| `RABBITMQ_DEAD_LETTER_EXCHANGE` | `<exchange>.dlx` | Dead-letter exchange |
| `PUBSUB_PREFETCH` | `16` | Mensajes sin ackear por consumer |
| `PUBSUB_MAX_RETRIES` | `3` | Reintentos ante un fallo transitorio |
| `PUBSUB_RETRY_INITIAL_DELAY` | `200ms` | Primer backoff |
| `PUBSUB_RETRY_MAX_DELAY` | `5s` | Techo del backoff |
| `PUBSUB_PUBLISH_TIMEOUT` | `5s` | Espera de la confirmación del broker |
| `PUBSUB_RECONNECT_INITIAL_DELAY` | `500ms` | Primer backoff de reconexión |
| `PUBSUB_RECONNECT_MAX_DELAY` | `30s` | Techo del backoff de reconexión |

Un valor mal escrito **falla al arrancar** en vez de caer al default en
silencio: un `PUBSUB_PREFETCH=muchos` ignorado sin avisar se descubre en
producción como un consumo raro, no como un error.

El default de `RABBITMQ_URL` no lleva credenciales a propósito:
`guest:guest` es el default del propio RabbitMQ para conexiones locales, así
el repositorio no tiene ni credenciales de mentira escritas. En cualquier
otro entorno la URL sale del sistema de secretos.

### Política de `prefetch` y reintentos

ADR-0003 dejó estos dos números explícitamente pendientes "al implementar
INF-06". Quedan así:

- **`prefetch = 16`.** Es el límite de mensajes sin ackear por consumer. Con
  16, una instancia que se cae devuelve a la cola a lo sumo 16 eventos para
  que otra los tome, y alcanza para que el consumo no quede serializado por
  la latencia de cada handler. Un consumer con handlers lentos puede bajarlo
  con `Subscription.Prefetch`.
- **3 reintentos, backoff exponencial de 200 ms a 5 s, con jitter.** Cubre el
  caso típico —una dependencia que parpadea— con una espera acotada: los tres
  backoffs son 200, 400 y 800 ms, así que un evento espera **1,4 s como
  máximo** antes de la DLQ, más lo que tarden sus cuatro ejecuciones del
  handler. Con el jitter cada espera cae entre la mitad y el total, así que
  el piso son 700 ms. El techo de 5 s no llega a aplicar con 3 reintentos:
  está para el consumer que suba `PUBSUB_MAX_RETRIES`. El jitter importa
  porque el fallo transitorio típico es una dependencia compartida caída: sin
  él, las N instancias reintentan todas en el mismo instante y le pegan al
  recurso justo cuando se está recuperando.
- **El reintento es en proceso**, y recién al agotarse el mensaje se rechaza
  hacia el dead-letter exchange. ADR-0003 mencionaba una cola de retry con
  TTL + DLX; se implementó lo que dice el contrato de eventos de INF-02, que
  es en proceso y no necesita dos exchanges y N colas de espera extra.

Cada cola compartida tiene su propia DLQ, `<cola>.dlq`. Una cola por
instancia **no** tiene: acumular eventos muertos de instancias que ya no
existen es basura, no resiliencia, y lo que se pierde ahí se recupera del
historial persistido.

## Desarrollo

```bash
docker compose up -d      # RabbitMQ + management UI en localhost:15672

make test                 # Go; los tests de integración se saltan si no hay broker
make test-integration     # Go contra el broker, con -race
make test-python          # Python en un container, sin instalar nada
make test-all             # las dos suites
make broker-down
```

Cobertura con el broker arriba: **91%** en Go, **99%** en Python. Sin broker,
los tests de integración se saltan y la de Go baja a ~68%: el gate del 70%
(DoD) se mide con el broker levantado, que es como corre CI.

## Diferencias entre los dos clientes

Son deliberadas: cada lenguaje hace lo idiomático, la semántica es la misma.

| | Go | Python |
| --- | --- | --- |
| Handler | Devuelve `error` | Levanta excepciones |
| Concurrencia | Bloqueante, una goroutine por subscripción | `async`/`await` |
| Reconexión | Supervisor propio con backoff | `aio_pika.connect_robust` |
| Opciones del sobre | Functional options | Argumentos por palabra clave |

En Python la reconexión no está escrita a mano porque `connect_robust` ya
rehace conexión, canales, colas y consumers. En Go no hay equivalente.

## Pendientes que este repo no cierra

- **Monitoreo de las DLQ:** quién las mira, con qué frecuencia y qué se hace
  con un evento muerto. El cliente las crea y loguea cada dead-letter, pero
  la operación es una decisión del equipo (queda de ADR-0003).
- **Broker gestionado para la nube** y su free tier: se define junto con
  INF-16.
- **Dos desvíos de ADR-0003 para llevar a la weekly**, los dos siguiendo el
  contrato de INF-02: un solo exchange con dos tipos de cola en vez de un
  `discordia.realtime` separado, y reintento en proceso en vez de cola de
  retry con TTL. Si el equipo prefiere lo que dice el ADR, se cambia acá y
  no en los ocho servicios — que es justamente para lo que existe este repo.

## Herramientas de Generación de código con IA (CLAUDE)

### Plugins de la comunidad

```bash
claude plugin marketplace add samber/cc
claude plugin install cc-skills-golang@samber
```

```bash
claude plugin marketplace add netresearch/claude-code-marketplace
claude plugin install git-workflow@netresearch-claude-code-marketplace
```

- **`cc-skills-golang`** ([samber/cc-skills-golang](https://github.com/samber/cc-skills-golang)):
  ~46 skills de Go idiomático. Se instala como un solo plugin —no hay forma
  de instalar solo un subconjunto por CLI— pero cada skill dispara sola según
  su propia descripción (`Apply when...`). Las que efectivamente aplican en
  una librería de mensajería como esta:

  | Skill | Por qué aplica acá |
  |---|---|
  | `golang-concurrency` | Consumers, supervisor de reconexión y shutdown, todo con goroutines propias |
  | `golang-safety` | Estado compartido entre el loop de consumo y el resto del proceso |
  | `golang-context` | Cancelación de publishes y de subscripciones vía `context` |
  | `golang-error-handling` | La distinción transitorio vs. permanente es el corazón de este cliente |
  | `golang-testing`, `golang-stretchr-testify` | Tests table-driven y el bus en memoria |
  | `golang-observability` | Logging estructurado de reintentos y dead-letters |
  | `golang-structs-interfaces`, `golang-naming`, `golang-documentation` | Es API pública: la usan otros seis repos |
  | `golang-dependency-management` | `go.mod`/`go.sum`, `govulncheck` |
  | `golang-security` | Credenciales del broker por entorno, nunca en el código |

- **`git-workflow`** ([netresearch/git-workflow-skill](https://github.com/netresearch/git-workflow-skill)):
  conventional commits, estrategias de branching, PR/review. Útil para
  mantener consistencia con
  [`discordia-docs/procesos/git-workflow.md`](https://github.com/Discordia-Grupo-16/discordia-docs/blob/dev/procesos/git-workflow.md)
  entre los distintos repos.

### Skills propias de este repo

En [`.claude/skills/`](.claude/skills/), cargadas automáticamente para
cualquiera que trabaje en este repo con Claude Code: topología de colas,
idempotencia y reintentos, convenciones de eventos, ruteo de ADRs y el
checklist de Definition of Done.
