# discordia-pubsub

Cliente compartido de Pub/Sub sobre RabbitMQ para los servicios de Discordia
(**INF-06**). Una sola implementación de publish/subscribe, en Go y en Python,
que todos los servicios usan en vez de hablarle a RabbitMQ por su cuenta.

> Este repositorio es una **librería**, no un servicio: no expone HTTP, no
> tiene base de datos y no se despliega solo.

## Por qué existe

[ADR-0003](https://github.com/Discordia-Grupo-16/discordia-docs/blob/dev/adr/0003-tecnologia-del-bus-pubsub.md)
eligió RabbitMQ y dejó dos topologías distintas sobre el mismo broker: colas
compartidas para eventos de dominio y colas por instancia para el fan-out de
mensajería. Confundirlas **no tira error**, solo hace que a algunos clientes
no les llegue nada. Este cliente encapsula esa diferencia detrás de una API
única para que ningún servicio la tenga que resolver de nuevo.

Además concentra, una vez y para los ocho servicios, el envelope de eventos de
INF-02, el ack manual, el `prefetch`, el backoff, la dead-letter queue y la
reconexión al broker.

## Estado

En construcción. La API, las variables de entorno y la política de reintentos
se documentan acá a medida que se implementan.

## Estructura

| Ruta | Qué es |
|---|---|
| `/` | Cliente Go — `import "github.com/Discordia-Grupo-16/pubsub"` |
| `python/` | Cliente Python — paquete `discordia_pubsub` |

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
  ~46 skills de Go idiomático. Se instala como un solo plugin — no hay forma
  de instalar solo un subconjunto por CLI — pero cada skill dispara sola
  según su propia descripción (`Apply when...`). Las que efectivamente van a
  disparar en una librería de mensajería como esta:

  | Skill | Por qué aplica acá |
  |---|---|
  | `golang-concurrency` | Consumers, reconexión y shutdown, todo con goroutines propias |
  | `golang-safety` | Estado compartido entre el loop de consumo y el resto del proceso |
  | `golang-context` | Cancelación de publishes y de subscripciones vía `context` |
  | `golang-error-handling` | La distinción transitorio vs. permanente es el corazón de este cliente |
  | `golang-testing`, `golang-stretchr-testify` | Tests table-driven y el fake en memoria |
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

En `.claude/skills/`, cargadas automáticamente para cualquiera que trabaje en
este repo con Claude Code: topología de colas, idempotencia y reintentos,
convenciones de eventos, ruteo de ADRs y el checklist de Definition of Done.
