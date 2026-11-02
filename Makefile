.PHONY: build test integration vet fmt proto up down demo bench
build:
	go build ./cmd/...
test:
	go test -race ./...
integration:
	test -n "$$TEST_DATABASE_URL"
	go test -race -count=1 ./internal/postgres ./internal/transport
vet:
	go vet ./...
fmt:
	gofmt -w cmd internal
proto:
	PATH="$$PWD/.tools:$$PATH" .tools/protoc/bin/protoc -I . --go_out=. --go_opt=module=github.com/helioLJ/csvcheck --go-grpc_out=. --go-grpc_opt=module=github.com/helioLJ/csvcheck api/csvcheck/v1/service.proto
up:
	docker compose up --build -d --scale worker=2
down:
	docker compose down
demo:
	mkdir -p reports
	docker compose run --rm --build --user "$$(id -u):$$(id -g)" cli demo -out /reports
bench:
	go test -run '^$$' -bench . -benchmem ./internal/validation
