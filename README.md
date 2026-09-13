# discordia-chat

Servicio de mensajería en tiempo real de Discordia (Go + MongoDB + RabbitMQ +
WebSocket).

## Stack

- Go 1.23, `testify` para tests.
- MongoDB para persistencia (`internal/repository`).
- RabbitMQ como bus de eventos (`internal/bus`) — ver
  [`discordia-docs/arquitectura/eventos.md`](https://github.com/Discordia-Grupo-16/discordia-docs/blob/dev/arquitectura/eventos.md).
- Hub de WebSocket para tiempo real (`internal/realtime`).
- Config 100% por variables de entorno (`internal/config`, ver
  `.env.example`).
- Logging con `slog` (JSON).

## Cómo correrlo

```bash
cp .env.example .env
go run ./cmd/server
```

| Variable | Default | Para qué |
|---|---|---|
| `APP_ENV` | `development` | Entorno del proceso |
| `PORT` | `8080` | Puerto HTTP/WS |
| `MONGO_URI` | `mongodb://localhost:27017` | Conexión a MongoDB |
| `MONGO_DB_NAME` | `discordia_chat` | Base de Mongo |
| `LOG_LEVEL` | `info` | Nivel de `slog` |
| `MONGO_MIN_POOL_SIZE` | `0` | Conexiones mínimas que el driver mantiene abiertas |
| `MONGO_MAX_POOL_SIZE` | `100` | Tope del pool de conexiones a Mongo |
| `MONGO_CONNECT_TIMEOUT` | `5s` | Timeout de la conexión TCP inicial |
| `MONGO_SERVER_SELECTION_TIMEOUT` | `5s` | Timeout para que el driver encuentre un servidor apto |
| `MONGO_OPERATION_TIMEOUT` | `10s` | Timeout por defecto de cada operación (CSOT) |

```bash
make test         # go test ./...
make test-cover    # cobertura; umbral 70% validado en CI
```

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
  según su propia descripción (`Apply when...`), así que instalar el paquete
  completo no significa que las 46 se usen todo el tiempo. Las que
  efectivamente van a disparar en este código, por lo que ya usa o por lo que
  pide la [Definition of Done](https://github.com/Discordia-Grupo-16/discordia-docs/blob/dev/procesos/definition-of-done.md)
  del equipo:

  | Skill | Por qué aplica acá |
  |---|---|
  | `golang-stretchr-testify` | El repo ya usa `testify` (`internal/config/config_test.go`) |
  | `golang-testing` | Tests table-driven, cobertura, `t.Setenv` |
  | `golang-error-handling` | Manejo de errores en las cuatro capas de `internal/` |
  | `golang-safety` | Nil/maps concurrentes — relevante para el estado del hub de `internal/realtime` |
  | `golang-concurrency` | Hub de WebSocket + consumers de RabbitMQ, ambos con goroutines propias |
  | `golang-observability` | El logger de `cmd/server/main.go` ya es `slog` |
  | `golang-security` | Secretos, PII en logs — la red line del [ADR-0007](https://github.com/Discordia-Grupo-16/discordia-docs/blob/dev/adr/0007-gestion-de-secretos.md) |
  | `golang-code-style`, `golang-naming`, `golang-documentation`, `golang-structs-interfaces` | Calidad de código general |
  | `golang-context` | Propagación de `context` entre `transport` → `domain`/`repository`/`bus` |
  | `golang-lint` | Legibilidad de código |
  | `golang-continuous-integration` | CI/Dockerfile |
  | `golang-dependency-management` | `go.mod`/`go.sum`, `govulncheck` |
  | `golang-project-layout` | Refuerza el layout `cmd/internal` ya adoptado |
  | `golang-swagger` | La DoD exige contrato OpenAPI para los endpoints que expone el servicio |
  | `golang-design-patterns` | Graceful shutdown de Mongo/RabbitMQ/WS al cerrar el proceso |


- **`git-workflow`** ([netresearch/git-workflow-skill](https://github.com/netresearch/git-workflow-skill)):
  conventional commits, estrategias de branching, PR/review. Útil para
  mantener consistencia con
  [`discordia-docs/procesos/git-workflow.md`](https://github.com/Discordia-Grupo-16/discordia-docs/blob/dev/procesos/git-workflow.md)
  entre los distintos repos de servicio.

### Skills propias de este repo

En `.claude/skills/`, cargadas automáticamente para cualquiera que trabaje en
este repo con Claude Code:

| Skill | Uso / propósito |
|---|---|
| [`discordia-rabbitmq-topology`](.claude/skills/discordia-rabbitmq-topology/SKILL.md) | Al declarar una cola o consumer en `internal/bus`: cuándo usar cola compartida (proyecciones que escriben en Mongo) vs. cola exclusive/auto-delete por instancia (`chat.message.sent`, fan-out a los clientes WebSocket de cada instancia). |
| [`discordia-event-idempotency`](.claude/skills/discordia-event-idempotency/SKILL.md) | Al escribir un handler de evento: idempotencia por `eventId` en inserts, patrón `lastEventAt` en proyecciones locales, ack manual post-proceso, backoff y dead-letter queue. |
| [`discordia-dod-checklist`](.claude/skills/discordia-dod-checklist/SKILL.md) | Antes de pedir review o cerrar una historia: cobertura que no baje, sin secretos, evento nuevo agregado a `eventos.md`, contrato OpenAPI, README con env vars. |
| [`discordia-adr-routing`](.claude/skills/discordia-adr-routing/SKILL.md) | Al evaluar una decisión de diseño: si hace falta un ADR, y si va en `discordia-docs/adr` (transversal) o en el `adr/` de este servicio, con qué numeración. |
| [`discordia-api-event-conventions`](.claude/skills/discordia-api-event-conventions/SKILL.md) | Al agregar un endpoint HTTP o un evento nuevo: recursos en plural, `camelCase`, forma de error estándar, naming `<servicio>.<agregado>.<evento-en-pasado>`, UUID v4, fechas UTC con `Z`. |
