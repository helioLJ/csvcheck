# Local operations

## Configuration

| Variable | Component | Default or requirement |
|---|---|---|
| `DATABASE_URL` | Coordinator | Required PostgreSQL connection URL |
| `CLIENT_TOKEN` | Coordinator and CLI | Required. Coordinator requires at least 12 characters |
| `WORKER_TOKEN` | Coordinator and worker | Required. At least 12 characters, different from the client token |
| `LISTEN_ADDR` | Coordinator | `127.0.0.1:50051` |
| `COORDINATOR_ADDR` | Worker and CLI | `localhost:50051` |
| `DATA_DIR` | Coordinator and worker | `./data` |
| `LEASE_DURATION` | Coordinator | `15s`, minimum `3s` |
| `WORKER_ID` | Worker | Random process identifier |
| `TEST_DATABASE_URL` | Integration tests | Optional, explicit test database URL |

Compose supplies local demo tokens and a local database password.
These values are examples, not credentials for an external service.

## Start and inspect

```bash
make up
docker compose ps
docker compose logs -f coordinator worker
```

Unary RPC logs include the method, duration, and status code.
Worker logs include job and attempt IDs. Tokens and CSV values do not enter these logs.
The MVP has no metrics endpoint or distributed tracing exporter.

The initial schema runs in the `migrate` service before the coordinator starts.
Future schema changes require explicit versioned migrations.

## Exercise recovery

Build the local CLI:

```bash
go build -o bin/csvcheck ./cmd/csvcheck
export CLIENT_TOKEN=local-client-demo-token
mkdir -p reports
bin/csvcheck generate -rows 8000000 -out reports/large.csv
```

Start a large job in one terminal:

```bash
bin/csvcheck demo -file reports/large.csv \
  -rules examples/financial/rules.json -out reports/recovery -timeout 30m
```

The command prints its job ID. If the job is still `RUNNING`, stop the workers in another terminal:

```bash
docker compose kill worker
```

Restart the workers:

```bash
docker compose up -d --scale worker=2 worker
```

After lease expiry, the coordinator creates another attempt.
The watch stream shows `lease_expired`, `started`, and `completed` events.
The final report contains 8,000,000 records and 800,000 invalid records.
Recovery restarts the file from the beginning. The MVP does not resume at a byte offset.

## Shutdown and storage

SIGTERM cancels worker processing. The coordinator waits up to five seconds for RPC shutdown, then closes remaining calls.
Unfinished jobs recover through lease expiry after restart.

```bash
make down
```

This command retains named volumes. Uploads, reports, and events remain until an operator removes them.
Automatic cleanup, quotas, and retention are future work.

## Deployment boundary

The stack binds its public port to localhost. Workers and the coordinator share a trusted local volume.
Storage keys stay within that volume. Filesystem access assumes that other local processes do not insert symlinks or modify published files.

The current transport uses plaintext gRPC. An external deployment needs TLS, identity-based authorization, and storage isolation.
The two role tokens do not provide tenant isolation or per-worker identity.
The stack also needs backup, retention, metrics, and a deployment-specific capacity limit.
