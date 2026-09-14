.PHONY: test test-cover test-integration test-python test-all broker-up broker-down

# Los tests de integración se saltan solos si no hay un broker alcanzable, así
# que `make test` anda igual sin Docker levantado.
test:
	go test ./...

# Cobertura con reporte por función; el umbral del 70% (DoD) se valida en CI
# (INF-07) con el broker levantado, que es como se mide de verdad: sin él, los
# tests de integración se saltan y la cobertura baja.
#
# Sin -race: en algunos entornos Windows con mingw/gcc desalineado el race
# detector rompe con exit status 0xc0000139 sin relación con el código. Necesita
# CGO + un gcc alineado con la imagen de Go, así que corre en CI, que es Linux.
test-cover:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out

# Suite de Go contra el RabbitMQ de docker-compose.
test-integration: broker-up
	go test ./... -count=1 -race

# Suite de Python en un container, para no pedir un intérprete ni un venv en la
# máquina de nadie.
test-python:
	docker compose run --rm python-tests

test-all: test-integration test-python

broker-up:
	docker compose up -d --wait rabbitmq

broker-down:
	docker compose down
