.PHONY: build docker test test-unit test-docker test-docker-down php-smoke lint

build:
	go build -o bin/mongobetween .

docker:
	docker-compose up

# Full suite, always inside the docker harness (never against a MongoDB on the host).
test: test-docker

# Host-only run: DB-backed tests skip unless CI=true, so this runs only DB-free unit tests. CI is unset explicitly
# so an exported CI=true (as on CI runners) can never point the DB tests at a MongoDB on the host.
test-unit:
	env -u CI go test -count 1 -race ./...

test-docker:
	./scripts/test-docker.sh

lint:
	GOGC=75 golangci-lint run --timeout 10m --concurrency 32 -v ./...

test-docker-down:
	docker compose -p mongobetween-test down -v

php-smoke:
	./scripts/php-smoke.sh
