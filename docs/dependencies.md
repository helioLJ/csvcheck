# Dependencies

The project uses three direct Go modules. Exact versions appear in `go.mod`, and `go.sum` records module checksums.

| Module | Version | Reason |
|---|---|---|
| `google.golang.org/grpc` | `v1.85.0` | RPC transport, streams, metadata, and health checks |
| `google.golang.org/protobuf` | `v1.36.12` | Generated message types and Protobuf JSON |
| `github.com/jackc/pgx/v5` | `v5.12.0` | PostgreSQL transactions and a bounded connection pool |

Tests use the Go standard library and the existing gRPC test transport.
The domain, CSV validator, and filesystem adapter use no external algorithm libraries.

Transitive runtime modules include the pgx pool and connection-file parsers, Go network/text/system packages, and Google RPC types.
Some upstream test modules also appear in `go.sum`. They are not direct application dependencies.

```bash
go mod verify
go list -m all
```

Generated bindings use `protoc` 33.0, `protoc-gen-go` 1.36.12, and `protoc-gen-go-grpc` 1.6.2.
The installation script checks compiler archive hashes from the official GitHub release metadata.
GitHub Actions use commit hashes. Container tags use explicit Go and PostgreSQL versions.
The Debian base tag remains mutable and needs regular maintenance.
