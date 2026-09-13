.PHONY: test test-cover

test:
	go test ./...

# Cobertura con reporte por función; el umbral del 70% (DoD) se valida en CI (INF-07).
# Sin -race: en algunos entornos Windows con mingw/gcc desalineado el race detector
# rompe con exit status 0xc0000139 sin relación con el código (probado con un módulo
# vacío). Necesita CGO + un gcc alineado con la imagen de Go, así que se retoma con
# -race dentro del container (o en CI, que corre Linux) cuando esté el Dockerfile
# de SCRUM-129 — acá en Windows no es confiable.
test-cover:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out

# Lint, Dockerfile, docker-compose y el target de test con -race dentro del
# container llegan con INF-09 (SCRUM-129).
