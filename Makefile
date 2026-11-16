.PHONY: build docker test test-docker test-docker-down lint

build:
	go build -o bin/mongobetween .

docker:
	docker-compose up

test:
	go test -count 1 -race ./...

test-docker:
	./scripts/test-docker.sh

lint:
	GOGC=75 golangci-lint run --timeout 10m --concurrency 32 -v -E golint ./...

test-docker-down:
	docker compose -p mongobetween-test down -v
