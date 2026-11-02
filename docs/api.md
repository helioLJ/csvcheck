# API and CLI

The source contract is [service.proto](../api/csvcheck/v1/service.proto).
All RPCs require a bearer token in the `authorization` metadata.
Public RPCs use `CLIENT_TOKEN`. Worker RPCs use `WORKER_TOKEN`.
These tokens separate roles, not individual users or workers.
The standard gRPC health service uses the client token. Compose waits for readiness before it starts workers.

## Public service

| RPC | Type | Behavior |
|---|---|---|
| `UploadFile` | Client stream | Metadata first, then chunks of at most 64 KiB |
| `SubmitJob` | Unary | Immutable file/rules pair with an idempotency key |
| `GetJob` | Unary | Current typed job snapshot |
| `WatchJob` | Server stream | Events after a sequence, followed by new events |
| `CancelJob` | Unary | Idempotent cancellation or existing terminal state |
| `DownloadArtifact` | Server stream | Published `summary` or `errors` artifact |

Uploads accept at most 1 GiB. The server generates file IDs and storage keys.
Original filenames are display metadata. The server rejects path separators and never uses the name as a path.

## Worker service

| RPC | Behavior |
|---|---|
| `ClaimJob` | One queued job or an empty response |
| `Heartbeat` | Monotonic progress and lease renewal |
| `CompleteAttempt` | Summary and two complete, hashed artifacts |
| `FailAttempt` | `INVALID_CSV` or `IO_ERROR` |

Workers poll for jobs every 500 milliseconds. A worker processes one job at a time.
The MVP does not use long polling or bidirectional streams.
A missing or stale lease makes the heartbeat response request cancellation.

`IO_ERROR` permits another attempt. `INVALID_CSV` fails the job permanently.
A coordinator restart retains jobs, attempts, and events in PostgreSQL.

## CSV semantics

- Encoding: UTF-8, without a byte-order mark.
- Delimiters: comma, semicolon, or tab.
- Header: required, with nonempty, unique column names.
- Records: at most 1 MiB and 1,024 columns.
- Empty physical lines: ignored by the CSV parser.
- Quoted fields: support embedded delimiters and newlines.
- Names and values: exact, without whitespace removal.
- Empty optional values: skip type constraints.
- Decimal values: optional minus sign, digits, and an optional decimal fraction.
- Decimal precision: at most 128 bytes per value. Exponents, fractions, `NaN`, and infinity are invalid.
- Dates: `YYYY-MM-DD` only.
- Text length: Unicode code points, not bytes or grapheme clusters.

A record can produce several violations. `invalid_records` counts each affected record once.
Missing required columns produce separate header violations at record zero.
Report record numbers identify logical data records, not physical lines.
The error sample includes column names and error codes, without source values.

## Status codes

| Code | Meaning |
|---|---|
| `INVALID_ARGUMENT` | Invalid rules, progress, cursor, or stream message order |
| `NOT_FOUND` | Unknown file, job, or published artifact |
| `ALREADY_EXISTS` | Idempotency key or completion conflicts with existing data |
| `FAILED_PRECONDITION` | Attempt lost ownership |
| `UNAUTHENTICATED` | Missing token or wrong role token |
| `DEADLINE_EXCEEDED` | Client deadline expired |
| `RESOURCE_EXHAUSTED` | Upload exceeds the file limit |
| `INTERNAL` | Unexpected storage or database error |

A timeout does not prove that a write failed. Repeat a submission with the same key.
The worker repeats uncertain completion calls up to three times with the same payload.
Other transient failures rely on lease expiry and reassignment.

## CLI workflow

```bash
export CLIENT_TOKEN=local-client-demo-token
go build -o bin/csvcheck ./cmd/csvcheck
bin/csvcheck upload -file examples/financial/transactions.csv
```

Use the returned file ID:

```bash
bin/csvcheck submit -file-id FILE_ID -key financial-example-1 \
  -rules examples/financial/rules.json
bin/csvcheck watch -job JOB_ID
bin/csvcheck get -job JOB_ID
bin/csvcheck download -job JOB_ID -kind summary -out summary.json
bin/csvcheck download -job JOB_ID -kind errors -out errors.csv
```

To resume observation, supply the last received sequence:

```bash
bin/csvcheck watch -job JOB_ID -after 2
bin/csvcheck cancel -job JOB_ID
```

The CLI uses a ten-minute command deadline. Override it with `-timeout 30m` for larger inputs.
Protobuf JSON represents 64-bit counters as strings. The downloaded report uses ordinary JSON numbers.

## Regenerate bindings

Generated bindings are committed. Normal builds do not need `protoc`.

```bash
scripts/install-proto-tools.sh
make proto
git diff -- api/csvcheck/v1
```

The script installs pinned generators into `.tools`. It downloads the official Protobuf 33.0 compiler and checks its SHA-256 digest.
