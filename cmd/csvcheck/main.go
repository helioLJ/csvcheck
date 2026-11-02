// csvcheck is the demonstration client. It never accesses PostgreSQL directly.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	pb "github.com/helioLJ/csvcheck/api/csvcheck/v1"
	"github.com/helioLJ/csvcheck/internal/config"
	"github.com/helioLJ/csvcheck/internal/domain"
	"github.com/helioLJ/csvcheck/internal/transport"
	"github.com/helioLJ/csvcheck/internal/validation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: csvcheck <upload|submit|get|watch|cancel|download|generate|demo|health> [flags]")
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	addr := flags.String("addr", config.Env("COORDINATOR_ADDR", "localhost:50051"), "Coordinator address")
	token := flags.String("token", os.Getenv("CLIENT_TOKEN"), "Client bearer token (prefer CLIENT_TOKEN)")
	timeout := flags.Duration("timeout", 10*time.Minute, "Overall command deadline")
	input := flags.String("file", "examples/financial/transactions.csv", "CSV input path")
	rules := flags.String("rules", "examples/financial/rules.json", "Validation configuration path")
	fileID := flags.String("file-id", "", "Uploaded file ID")
	jobID := flags.String("job", "", "Job ID")
	key := flags.String("key", "", "Submission idempotency key")
	after := flags.Int64("after", 0, "Last event sequence already received")
	kind := flags.String("kind", "summary", "Artifact kind: summary or errors")
	output := flags.String("out", "report.json", "Output path (demo uses a directory)")
	rows := flags.Int("rows", 100000, "Number of synthetic records")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if command == "generate" {
		return generate(*output, *rows)
	}
	if *token == "" {
		return errors.New("set CLIENT_TOKEN or pass -token")
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	ctx = transport.Outgoing(ctx, *token)
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewValidationServiceClient(conn)
	switch command {
	case "health":
		response, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if response.Status != healthpb.HealthCheckResponse_SERVING {
			return errors.New("coordinator is not ready")
		}
		return nil
	case "upload":
		f, err := upload(ctx, client, *input)
		if err != nil {
			return err
		}
		return printJSON(f)
	case "submit":
		if *key == "" {
			return errors.New("-key is required for repeatable submission")
		}
		c, err := readRules(*rules)
		if err != nil {
			return err
		}
		j, err := client.SubmitJob(ctx, &pb.SubmitJobRequest{IdempotencyKey: *key, FileId: *fileID, Config: transport.ProtoConfig(c)})
		if err != nil {
			return err
		}
		fmt.Println(protoJSON(j))
		return nil
	case "get", "cancel":
		var j *pb.Job
		if command == "get" {
			j, err = client.GetJob(ctx, &pb.GetJobRequest{JobId: *jobID})
		} else {
			j, err = client.CancelJob(ctx, &pb.GetJobRequest{JobId: *jobID})
		}
		if err != nil {
			return err
		}
		fmt.Println(protoJSON(j))
		return nil
	case "watch":
		_, err = watch(ctx, client, *jobID, *after)
		return err
	case "download":
		return download(ctx, client, *jobID, *kind, *output)
	case "demo":
		c, err := readRules(*rules)
		if err != nil {
			return err
		}
		f, err := upload(ctx, client, *input)
		if err != nil {
			return err
		}
		j, err := client.SubmitJob(ctx, &pb.SubmitJobRequest{IdempotencyKey: domain.ID(), FileId: f.Id, Config: transport.ProtoConfig(c)})
		if err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "job:", j.Id)
		final, err := watch(ctx, client, j.Id, 0)
		if err != nil {
			return err
		}
		if final.State != "SUCCEEDED" {
			return fmt.Errorf("job ended in %s", final.State)
		}
		if err = os.MkdirAll(*output, 0700); err != nil {
			return err
		}
		for _, a := range []struct{ kind, name string }{{"summary", "summary.json"}, {"errors", "errors.csv"}} {
			if err = download(ctx, client, j.Id, a.kind, filepath.Join(*output, a.name)); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}
func printJSON(v any) error {
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	return e.Encode(v)
}
func readRules(path string) (domain.Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return domain.Config{}, err
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 1<<20))
	decoder.DisallowUnknownFields()
	var c domain.Config
	if err = decoder.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return c, errors.New("rules must contain one JSON document")
	}
	return c, validation.CheckConfig(c)
}
func upload(ctx context.Context, c pb.ValidationServiceClient, path string) (*pb.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stream, err := c.UploadFile(ctx)
	if err != nil {
		return nil, err
	}
	if err = stream.Send(&pb.UploadFileRequest{Payload: &pb.UploadFileRequest_Metadata{Metadata: &pb.UploadMetadata{Filename: filepath.Base(path)}}}); err != nil {
		return nil, err
	}
	buf := make([]byte, 64<<10)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err = stream.Send(&pb.UploadFileRequest{Payload: &pb.UploadFileRequest_Chunk{Chunk: buf[:n]}}); err != nil {
				return nil, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return stream.CloseAndRecv()
}
func watch(ctx context.Context, c pb.ValidationServiceClient, id string, after int64) (*pb.Job, error) {
	stream, err := c.WatchJob(ctx, &pb.WatchJobRequest{JobId: id, AfterSequence: after})
	if err != nil {
		return nil, err
	}
	var last *pb.Job
	for {
		e, err := stream.Recv()
		if err == io.EOF {
			if last == nil {
				return c.GetJob(ctx, &pb.GetJobRequest{JobId: id})
			}
			return last, nil
		}
		if err != nil {
			return nil, err
		}
		last = e.Job
		fmt.Printf("{\"sequence\":%d,\"kind\":%q,\"job\":%s}\n", e.Sequence, e.Kind, protoJSON(e.Job))
	}
}
func download(ctx context.Context, c pb.ValidationServiceClient, id, kind, path string) error {
	stream, err := c.DownloadArtifact(ctx, &pb.DownloadArtifactRequest{JobId: id, Kind: kind})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".csvcheck-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err = f.Write(chunk.Data); err != nil {
			return err
		}
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
func generate(path string, rows int) error {
	if rows < 1 || rows > 100000000 {
		return errors.New("rows must be between 1 and 100000000")
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err = w.Write([]string{"transaction_id", "amount", "created_at"}); err != nil {
		return err
	}
	for i := 1; i <= rows; i++ {
		amount := "12.50"
		if i%10 == 0 {
			amount = "-1.00"
		}
		if err = w.Write([]string{strconv.Itoa(i), amount, "2026-10-28"}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func protoJSON(p proto.Message) string {
	b, _ := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(p)
	return string(b)
}
