# Use cases and tests

## Acceptance cases

| ID | Use case | Expected result | Evidence |
|---|---|---|---|
| UC-01 | Analyze a CSV with invalid values | Complete analysis, exact counts, downloadable reports | `TestEndToEnd` |
| UC-02 | Repeat a submission | Return one job for the same key and payload | `TestConcurrentSubmitIdempotency` |
| UC-03 | Change an existing submission | Reject the conflicting request | `TestConcurrentSubmitIdempotency` |
| UC-04 | Resume observation | Replay events without sequence gaps | `TestEndToEnd` |
| UC-05 | Stop observation | Keep the job active | `TestAuthCancellationAndDeadline` |
| UC-06 | Cancel a job | Reject later completion | `TestCompletionReplayCancelAndEvents` |
| UC-07 | Lose a worker | Reassign the job and publish only the replacement result | `TestRecoveryPublishesOnlyReplacementAttempt` |
| UC-08 | Exhaust recovery attempts | Fail after three expired attempts | `TestLeaseExpiryExhaustsAttempts` |
| UC-09 | Repeat a completion | Return the original result without another event | `TestCompletionReplayCancelAndEvents` |
| UC-10 | Fail during publication | Roll back all artifact references and state changes | `TestRollbackDoesNotPublishHalfCompletion` |
| UC-11 | Interrupt an upload | Publish no partial file | `TestAtomicPublication` |
| UC-12 | Call a worker RPC with a client token | Reject authentication | `TestAuthCancellationAndDeadline` |

## Test layers

- [Validation tests](../internal/validation/engine_test.go): decimals, Unicode, dates, quoted records, invalid headers, sample limits, and cancellation.
- [Storage tests](../internal/storage/local_test.go): hashes, atomic publication, and path traversal rejection.
- [PostgreSQL tests](../internal/postgres/store_test.go): concurrent claims, idempotency, rollback, leases, cancellation, and retries.
- [gRPC tests](../internal/transport/server_test.go): complete workflows with real PostgreSQL, an in-process gRPC server, and a real worker loop.

The gRPC tests use `bufconn` for transport. The Docker demo exercises TCP between separate processes.
Database tests use isolated schemas. They never truncate an existing application schema.

## Run the checks

```bash
make test
make vet
go mod verify
```

Without `TEST_DATABASE_URL`, database and end-to-end tests report skips.
For the full suite, start PostgreSQL and set the URL:

```bash
docker compose -f compose.yaml -f compose.test.yaml up -d --wait postgres
export TEST_DATABASE_URL='postgres://csvcheck:local-demo-only@localhost:55432/csvcheck?sslmode=disable'
go test -race -count=1 ./...
```

The race detector observes memory races during these tests. It does not prove all possible interleavings are correct.
Database tests use concurrent connections to exercise ownership rules.

## Fuzz the parser

```bash
go test -run '^$' -fuzz FuzzRun -fuzztime=10s ./internal/validation
```

The fuzz target accepts arbitrary input under a fixed size cap.
The parser must return a result or an error without a panic.

## Financial example

The example uses synthetic data. It has no connection to a bank or payment processor.

| Record | Problem |
|---|---|
| 1 | None |
| 2 | Negative amount |
| 3 | Invalid calendar date |
| 4 | Missing transaction ID and zero amount |
| 5 | None |

Expected totals: five records, three invalid records, four violations, and four samples.
The `errors.csv` report excludes source values.
