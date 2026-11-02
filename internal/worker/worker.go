// Package worker executes one assignment at a time and renews its lease.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/helioLJ/csvcheck/api/csvcheck/v1"
	"github.com/helioLJ/csvcheck/internal/domain"
	"github.com/helioLJ/csvcheck/internal/storage"
	"github.com/helioLJ/csvcheck/internal/transport"
	"github.com/helioLJ/csvcheck/internal/validation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Worker struct {
	Client            pb.WorkerServiceClient
	Files             *storage.Local
	ID                string
	HeartbeatInterval time.Duration
}

func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		response, err := w.Client.ClaimJob(call, &pb.ClaimJobRequest{WorkerId: w.ID})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("claim failed", "error", err)
		} else if a := response.Assignment; a != nil {
			w.process(ctx, a)
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil
}
func (w *Worker) process(parent context.Context, a *pb.Assignment) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var mu sync.Mutex
	var progress domain.Progress
	ref := &pb.AttemptRef{JobId: a.JobId, AttemptId: a.AttemptId, LeaseToken: a.LeaseToken}
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		interval := w.HeartbeatInterval
		if interval <= 0 {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mu.Lock()
				p := progress
				mu.Unlock()
				call, stop := context.WithTimeout(ctx, 2*time.Second)
				reply, err := w.Client.Heartbeat(call, &pb.HeartbeatRequest{Attempt: ref, Progress: &pb.Progress{BytesConsumed: p.BytesConsumed, RecordsProcessed: p.RecordsProcessed}})
				stop()
				// Stop work on uncertainty. Expiry and reassignment recover it safely.
				if err != nil || reply.CancelRequested {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-heartbeatDone }()
	slog.Info("attempt started", "job_id", a.JobId, "attempt_id", a.AttemptId, "worker_id", w.ID)
	sum, artifacts, err := w.execute(ctx, a, func(p domain.Progress) { mu.Lock(); progress = p; mu.Unlock() })
	if ctx.Err() != nil {
		slog.Info("attempt stopped", "job_id", a.JobId)
		return
	}
	if err != nil {
		code := "IO_ERROR"
		var malformed *CSVError
		if errors.As(err, &malformed) {
			code = "INVALID_CSV"
		}
		message := err.Error()
		if len(message) > 500 {
			message = message[:500]
		}
		call, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		if _, err = w.Client.FailAttempt(call, &pb.FailAttemptRequest{Attempt: ref, Code: code, Message: message}); err != nil {
			slog.Warn("failure report failed", "job_id", a.JobId, "error", err)
		}
		return
	}
	request := &pb.CompleteAttemptRequest{Attempt: ref, Summary: transport.ProtoSummary(sum), Artifacts: artifacts}
	for attempt := 0; attempt < 3; attempt++ {
		call, stop := context.WithTimeout(ctx, 5*time.Second)
		job, err := w.Client.CompleteAttempt(call, request)
		stop()
		if err == nil {
			slog.Info("attempt completed", "job_id", job.Id, "outcome", job.ValidationOutcome)
			return
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			slog.Warn("completion rejected", "job_id", a.JobId, "error", err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
		}
	}
}

type CSVError struct{ Err error }

func (e *CSVError) Error() string { return e.Err.Error() }
func (w *Worker) execute(ctx context.Context, a *pb.Assignment, progress func(domain.Progress)) (domain.Summary, []*pb.Artifact, error) {
	input, err := w.Files.Open(a.InputKey)
	if err != nil {
		return domain.Summary{}, nil, err
	}
	defer input.Close()
	var sample bytes.Buffer
	sum, err := validation.Run(ctx, input, transport.Config(a.Config), &sample, progress)
	if err != nil {
		return sum, nil, &CSVError{Err: err}
	}
	report := struct {
		Version         int            `json:"version"`
		Config          domain.Config  `json:"config"`
		Summary         domain.Summary `json:"summary"`
		SampleTruncated bool           `json:"sample_truncated"`
	}{1, transport.Config(a.Config), sum, sum.SampledViolations < sum.Violations}
	reportBytes, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return sum, nil, err
	}
	var artifacts []*pb.Artifact
	for _, item := range []struct {
		kind, name string
		data       []byte
	}{{"summary", "summary.json", reportBytes}, {"errors", "errors.csv", sample.Bytes()}} {
		key := fmt.Sprintf("%s/%s", a.OutputPrefix, item.name)
		size, digest, err := w.Files.Put(ctx, key, bytes.NewReader(item.data))
		if err != nil {
			return sum, nil, err
		}
		artifacts = append(artifacts, &pb.Artifact{Kind: item.kind, Key: key, SizeBytes: size, Sha256: digest})
	}
	return sum, artifacts, nil
}
