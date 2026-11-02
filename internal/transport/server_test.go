package transport_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/helioLJ/csvcheck/api/csvcheck/v1"
	"github.com/helioLJ/csvcheck/internal/coordinator"
	"github.com/helioLJ/csvcheck/internal/domain"
	"github.com/helioLJ/csvcheck/internal/postgres"
	"github.com/helioLJ/csvcheck/internal/storage"
	"github.com/helioLJ/csvcheck/internal/transport"
	"github.com/helioLJ/csvcheck/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func fixture(t *testing.T) (*grpc.ClientConn, *coordinator.Service) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL for end-to-end tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	name := "test_" + domain.ID()
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	store := &postgres.Store{Pool: pool, Lease: 15 * time.Second}
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	files, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service := &coordinator.Service{Repo: store, Files: files}
	handler := &transport.Server{Service: service}
	server := grpc.NewServer(grpc.UnaryInterceptor(transport.UnaryAuth("client-secret", "worker-secret")), grpc.StreamInterceptor(transport.StreamAuth("client-secret", "worker-secret")))
	pb.RegisterValidationServiceServer(server, handler)
	pb.RegisterWorkerServiceServer(server, handler)
	listener := bufconn.Listen(2 << 20)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		server.Stop()
		listener.Close()
		pool.Close()
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
	return conn, service
}
func upload(t *testing.T, ctx context.Context, c pb.ValidationServiceClient, input string) *pb.File {
	t.Helper()
	stream, err := c.UploadFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(&pb.UploadFileRequest{Payload: &pb.UploadFileRequest_Metadata{Metadata: &pb.UploadMetadata{Filename: "example.csv"}}}); err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(&pb.UploadFileRequest{Payload: &pb.UploadFileRequest_Chunk{Chunk: []byte(input)}}); err != nil {
		t.Fatal(err)
	}
	f, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func rule() *pb.ValidationConfig {
	return &pb.ValidationConfig{Delimiter: ",", Rules: []*pb.ColumnRule{{Column: "amount", RequiredColumn: true, RequiredValue: true, Constraint: &pb.ColumnRule_Decimal{Decimal: &pb.DecimalConstraint{}}}}}
}
func TestEndToEnd(t *testing.T) {
	conn, service := fixture(t)
	base, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	ctx := transport.Outgoing(base, "client-secret")
	client := pb.NewValidationServiceClient(conn)
	f := upload(t, ctx, client, "amount\n12.50\ninvalid\n")
	request := &pb.SubmitJobRequest{IdempotencyKey: "example-key", FileId: f.Id, Config: rule()}
	j, err := client.SubmitJob(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := client.SubmitJob(ctx, request)
	if err != nil || repeat.Id != j.Id {
		t.Fatal(repeat, err)
	}
	request.Config.Delimiter = ";"
	if _, err = client.SubmitJob(ctx, request); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed submission=%v", err)
	}
	workerCtx, cancel := context.WithCancel(transport.Outgoing(base, "worker-secret"))
	done := make(chan struct{})
	w := &worker.Worker{Client: pb.NewWorkerServiceClient(conn), Files: service.Files, ID: "test-worker", HeartbeatInterval: time.Second}
	go func() { defer close(done); _ = w.Run(workerCtx) }()
	defer func() { cancel(); <-done }()
	stream, err := client.WatchJob(ctx, &pb.WatchJobRequest{JobId: j.Id, AfterSequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	sequence := int64(1)
	var final *pb.Job
	for {
		e, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.Sequence != sequence+1 {
			t.Fatal("event gap")
		}
		sequence = e.Sequence
		final = e.Job
	}
	if final == nil || final.State != "SUCCEEDED" || final.ValidationOutcome != "FAILED_VALIDATION" || final.Summary.Records != 2 || final.Summary.InvalidRecords != 1 {
		t.Fatal(final)
	}
	download, err := client.DownloadArtifact(ctx, &pb.DownloadArtifactRequest{JobId: j.Id, Kind: "summary"})
	if err != nil {
		t.Fatal(err)
	}
	var report strings.Builder
	for {
		chunk, err := download.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		report.Write(chunk.Data)
	}
	var decoded struct {
		Summary domain.Summary `json:"summary"`
	}
	if err = json.Unmarshal([]byte(report.String()), &decoded); err != nil || decoded.Summary.Records != 2 {
		t.Fatal(report.String(), err)
	}
	resumed, err := client.WatchJob(ctx, &pb.WatchJobRequest{JobId: j.Id, AfterSequence: sequence})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = resumed.Recv(); err != io.EOF {
		t.Fatalf("resume=%v", err)
	}
}
func TestAuthCancellationAndDeadline(t *testing.T) {
	conn, _ := fixture(t)
	base, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := pb.NewValidationServiceClient(conn)
	ctx := transport.Outgoing(base, "client-secret")
	if _, err := client.GetJob(base, &pb.GetJobRequest{JobId: "unknown"}); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	if _, err := pb.NewWorkerServiceClient(conn).ClaimJob(ctx, &pb.ClaimJobRequest{WorkerId: "intruder"}); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	if _, err := client.GetJob(ctx, &pb.GetJobRequest{JobId: "unknown"}); status.Code(err) != codes.NotFound {
		t.Fatal(err)
	}
	f := upload(t, ctx, client, "amount\n1\n")
	j, err := client.SubmitJob(ctx, &pb.SubmitJobRequest{FileId: f.Id, IdempotencyKey: "cancel", Config: rule()})
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	stream, err := client.WatchJob(watchCtx, &pb.WatchJobRequest{JobId: j.Id, AfterSequence: j.LastEventSequence})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	current, err := client.GetJob(ctx, &pb.GetJobRequest{JobId: j.Id})
	if err != nil || current.State != "QUEUED" {
		t.Fatal(current, err)
	}
	cancelled, err := client.CancelJob(ctx, &pb.GetJobRequest{JobId: j.Id})
	if err != nil || cancelled.State != "CANCELLED" {
		t.Fatal(cancelled, err)
	}
}
func TestRejectIncompleteUploadAndMalformedCSV(t *testing.T) {
	conn, service := fixture(t)
	base, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx := transport.Outgoing(base, "client-secret")
	client := pb.NewValidationServiceClient(conn)
	stream, err := client.UploadFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&pb.UploadFileRequest{Payload: &pb.UploadFileRequest_Chunk{Chunk: []byte("wrong order")}})
	if _, err = stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	f := upload(t, ctx, client, "amount\n\"unterminated")
	j, err := client.SubmitJob(ctx, &pb.SubmitJobRequest{FileId: f.Id, IdempotencyKey: "malformed", Config: rule()})
	if err != nil {
		t.Fatal(err)
	}
	wc, stop := context.WithCancel(transport.Outgoing(base, "worker-secret"))
	done := make(chan struct{})
	defer func() { stop(); <-done }()
	w := worker.Worker{Client: pb.NewWorkerServiceClient(conn), Files: service.Files, ID: "worker"}
	go func() { defer close(done); _ = w.Run(wc) }()
	events, err := client.WatchJob(ctx, &pb.WatchJobRequest{JobId: j.Id})
	if err != nil {
		t.Fatal(err)
	}
	var final *pb.Job
	for {
		e, err := events.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		final = e.Job
	}
	if final.State != "FAILED" || !strings.HasPrefix(final.LastError, "INVALID_CSV") {
		t.Fatal(final)
	}
}
