.PHONY: test test-cover test-integration broker-up broker-down

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

# Suite completa contra el RabbitMQ de docker-compose.
test-integration: broker-up
	go test ./... -count=1 -race

broker-up:
	docker compose up -d --wait

broker-down:
	docker compose down
