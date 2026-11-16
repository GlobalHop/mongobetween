.PHONY: build docker test test-unit test-docker test-docker-down lint

build:
	go build -o bin/mongobetween .

docker:
	docker-compose up

# Full suite, always inside the docker harness (never against a MongoDB on the host).
test: test-docker

# Host-only run: DB-backed tests skip unless CI=true, so this runs only DB-free unit tests.
test-unit:
	go test -count 1 -race ./...

test-docker:
	./scripts/test-docker.sh

lint:
	GOGC=75 golangci-lint run --timeout 10m --concurrency 32 -v ./...

test-docker-down:
	docker compose -p mongobetween-test down -v
