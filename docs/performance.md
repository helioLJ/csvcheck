# Performance

## Questions to measure

1. Does worker memory remain bounded as the input file grows?
2. How much work does decimal validation allocate per record?
3. Does another worker improve throughput across independent files?
4. How long does recovery take after a worker stops?

A file belongs to one worker. More workers do not accelerate a single file.
PostgreSQL coordinates jobs and events. It does not store every record or violation.

## Parser benchmark

```bash
go test -run '^$' -bench BenchmarkValidate -benchmem -count=3 ./internal/validation
```

The benchmark validates 10,000 decimal records per operation.
It includes parsing, decimal conversion, and validation. It excludes gRPC, file storage, and PostgreSQL.
`B/op` measures total allocations, not peak resident memory.

Local sample, November 2, 2026: Apple M4 Max, macOS arm64, Go 1.26.1, default `GOMAXPROCS` of 16.

| Run | Time per 10,000 records | Throughput | Allocated bytes per operation |
|---|---|---|---|
| 1 | 4.34 ms | 13.83 MB/s | 3,613,909 |
| 2 | 4.41 ms | 13.60 MB/s | 3,613,945 |
| 3 | 4.24 ms | 14.15 MB/s | 3,613,879 |

These samples describe one local parser workload. They are not network or production capacity measurements.

## End-to-end workload

```bash
mkdir -p reports
go build -o bin/csvcheck ./cmd/csvcheck
bin/csvcheck generate -rows 1000000 -out reports/million.csv
export CLIENT_TOKEN=local-client-demo-token
time bin/csvcheck demo -file reports/million.csv \
  -rules examples/financial/rules.json -out reports/million
```

Expected totals: 1,000,000 records, 100,000 invalid records, and 1,000 samples.
The summary marks the sample as truncated.
This workload includes upload, queue wait, processing, publication, observation, and download.

Observe container memory in another terminal:

```bash
docker stats --no-stream
```

For a worker-count comparison, submit several independent files with one worker, then repeat with two workers.
Keep the input, machine, database, and container limits unchanged.
Record successful jobs, errors, elapsed time, and worker memory.
The repository makes no unmeasured claim about distributed throughput or production scale.

## Limits that bound memory

The validator reads one record at a time. Each record has a 1 MiB limit, plus a small parser read-ahead allowance.
The CSV parser accepts at most 1,024 columns. Decimal values have a 128-byte limit.
The error sample stops at 1,000 entries. Counts continue for the complete file.
Upload and download buffers contain at most 64 KiB per chunk.

Exact decimal validation uses rational arithmetic from the Go standard library.
This choice favors explicit decimal semantics. A benchmark can guide later representation changes.
