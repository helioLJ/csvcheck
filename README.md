# csvcheck

[![CI](https://github.com/helioLJ/csvcheck/actions/workflows/ci.yml/badge.svg)](https://github.com/helioLJ/csvcheck/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Validate large CSV files with Go workers, gRPC streams, and PostgreSQL leases.**

Submit a file, watch its progress, and download a report. Rules cover required columns, required values, decimal bounds, dates, and text length.

The project explores a specific distributed-systems problem: a worker can disappear while a job is active.
The coordinator reassigns expired attempts and rejects results from workers that lose ownership.

```mermaid
flowchart LR
    CLI[CLI] -->|Upload / submit / download| C[Coordinator]
    C -->|WatchJob stream| CLI
    W1[Worker A] -->|Claim / heartbeat / complete| C
    W2[Worker B] -->|Claim / heartbeat / complete| C
    C --> DB[(PostgreSQL)]
    C --> FS[(Shared file volume)]
    W1 --> FS
    W2 --> FS
```

## Try it

Requirements: Docker Engine with Compose. Go is optional for the container demo.

```bash
git clone git@github.com:helioLJ/csvcheck.git
cd csvcheck
make up
make demo
```

The financial example contains five records. The expected report has three invalid records and four violations.

```text
reports/
├── summary.json
└── errors.csv
```

A successful analysis can find invalid data. That job ends in `SUCCEEDED` with the outcome `FAILED_VALIDATION`.
A malformed CSV ends in `FAILED` because the analysis cannot finish.

```bash
# Keep the files and database for the next session.
make down
```

## What the project demonstrates

- **gRPC:** client streaming for uploads, server streaming for events and downloads, typed Protobuf contracts.
- **Go:** context cancellation, bounded buffers, goroutine coordination, and shutdown with a time limit.
- **Consistency:** submission idempotency, row locks, attempt tokens, and transactional event publication.
- **Recovery:** leases, at most three attempts, and rejection of stale completions.
- **Testing:** parser cases, PostgreSQL concurrency tests, and complete workflows over gRPC.

Each worker processes one file at a time. Additional workers process additional files, not partitions of one file.

## Use your own rules

```json
{
  "delimiter": ",",
  "rules": [
    {"column": "email", "required_column": true, "required_value": true},
    {"column": "age", "decimal": {"minimum": "18", "maximum": "120"}}
  ]
}
```

Rules use exact column names. Decimal bounds use strings to preserve precision.
The example requires an email value. It does not validate email syntax.

For a local CLI, use Go 1.26.1 or newer:

```bash
export CLIENT_TOKEN=local-client-demo-token
go build -o bin/csvcheck ./cmd/csvcheck
bin/csvcheck demo -file examples/financial/transactions.csv \
  -rules examples/financial/rules.json -out reports
```

## Project map

```text
api/csvcheck/v1/   Protobuf contracts and generated Go bindings
cmd/              Coordinator, worker, and CLI entry points
internal/domain/  Models and domain errors
internal/coordinator/  Job use cases and persistence/storage ports
internal/validation/   CSV parser and column rules
internal/postgres/     Transactions, leases, events, and initial schema
internal/storage/      Atomic local file publication
internal/transport/    gRPC handlers, authentication, and mapping
internal/worker/       Assignment execution and heartbeat loop
examples/financial/    Synthetic input and rules
```

## Development

```bash
make test
make vet
make bench
```

Unit tests run without PostgreSQL. Integration tests require an explicit database URL:

```bash
docker compose -f compose.yaml -f compose.test.yaml up -d postgres
export TEST_DATABASE_URL='postgres://csvcheck:local-demo-only@localhost:55432/csvcheck?sslmode=disable'
make integration
```

Each integration test uses a separate schema. The test account needs permission to create schemas.
CI runs integration tests, the race detector, parser fuzzing, and the container demo.

## Read more

| Document | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Boundaries, data model, states, and failure semantics |
| [API](docs/api.md) | RPC contracts, rules, errors, and CLI examples |
| [Use cases and tests](docs/testing.md) | Expected behavior and test coverage |
| [Operations](docs/operations.md) | Configuration, recovery exercise, and local limits |
| [Performance](docs/performance.md) | Reproduction commands and measured results |
| [Documentation review](docs/documentation-review.md) | Simple English review and scope |

## Scope

This repository is a local portfolio project. The Compose stack uses plaintext gRPC, documented demo tokens, and a shared volume.
It has no tenant isolation, production deployment, or automatic retention policy.
Files have a 1 GiB limit. CSV records have a 1 MiB limit, and reports contain at most 1,000 violation samples.

Design references include [BuildKit](https://github.com/moby/buildkit), [Temporal](https://github.com/temporalio/api), and [etcd](https://github.com/etcd-io/etcd).
The implementation is independent and deliberately smaller.
