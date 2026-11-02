// Package coordinator implements job use cases through persistence and storage ports.
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/helioLJ/csvcheck/internal/domain"
	"github.com/helioLJ/csvcheck/internal/storage"
	"github.com/helioLJ/csvcheck/internal/validation"
)

type Repository interface {
	AddFile(context.Context, domain.File) error
	File(context.Context, string) (domain.File, error)
	Submit(context.Context, string, string, domain.Config) (domain.Job, error)
	Get(context.Context, string) (domain.Job, error)
	Events(context.Context, string, int64) ([]domain.Event, error)
	Claim(context.Context, string) (*domain.Assignment, error)
	Heartbeat(context.Context, domain.AttemptRef, domain.Progress) (time.Time, error)
	Complete(context.Context, domain.AttemptRef, domain.Summary, []domain.Artifact) (domain.Job, error)
	Fail(context.Context, domain.AttemptRef, string, string) (domain.Job, error)
	Cancel(context.Context, string) (domain.Job, error)
	Artifact(context.Context, string, string) (domain.Artifact, error)
}
type Service struct {
	Repo  Repository
	Files *storage.Local
}

func invalid(message string) error { return fmt.Errorf("%w: %s", domain.ErrInvalid, message) }
func (s *Service) Upload(ctx context.Context, name string, reader io.Reader) (domain.File, error) {
	if name == "" || len(name) > 200 || strings.ContainsAny(name, "/\\\x00\r\n") {
		return domain.File{}, invalid("invalid display filename")
	}
	f := domain.File{ID: domain.ID(), Filename: name}
	f.Key = "inputs/" + f.ID + ".csv"
	var err error
	f.Size, f.SHA256, err = s.Files.Put(ctx, f.Key, reader)
	if err != nil {
		return f, err
	}
	return f, s.Repo.AddFile(ctx, f)
}
func (s *Service) Submit(ctx context.Context, key, file string, c domain.Config) (domain.Job, error) {
	if len(key) < 1 || len(key) > 128 || strings.TrimSpace(key) != key {
		return domain.Job{}, invalid("idempotency key must contain 1 to 128 characters without surrounding spaces")
	}
	if err := validation.CheckConfig(c); err != nil {
		return domain.Job{}, err
	}
	return s.Repo.Submit(ctx, key, file, c)
}
func (s *Service) Complete(ctx context.Context, r domain.AttemptRef, sum domain.Summary, artifacts []domain.Artifact) (domain.Job, error) {
	// Check ownership before reading artifact paths supplied by a worker.
	j, err := s.Repo.Get(ctx, r.JobID)
	if err != nil {
		return j, err
	}
	if j.CurrentAttempt != r.AttemptID {
		return j, domain.ErrStale
	}
	f, err := s.Repo.File(ctx, j.FileID)
	if err != nil {
		return j, err
	}
	if sum.Records < 0 || sum.InvalidRecords < 0 || sum.InvalidRecords > sum.Records || sum.Violations < sum.InvalidRecords || sum.SampledViolations < 0 || sum.SampledViolations > sum.Violations || sum.SampledViolations > validation.MaxSampleErrors || sum.BytesConsumed != f.Size {
		return j, invalid("inconsistent completion summary")
	}
	if len(artifacts) != 2 {
		return j, invalid("summary and errors artifacts are required")
	}
	seen := map[string]bool{}
	for _, a := range artifacts {
		filename := ""
		switch a.Kind {
		case "summary":
			filename = "summary.json"
		case "errors":
			filename = "errors.csv"
		default:
			return j, invalid("unknown artifact kind")
		}
		expected := path.Join("attempts", r.JobID, r.AttemptID, filename)
		if a.Key != expected || seen[a.Kind] {
			return j, invalid("artifact does not belong to this attempt")
		}
		seen[a.Kind] = true
		size, digest, err := s.Files.Inspect(ctx, a.Key)
		if err != nil {
			return j, err
		}
		if size != a.Size || digest != a.SHA256 {
			return j, invalid("artifact size or hash mismatch")
		}
		if a.Kind == "summary" {
			file, err := s.Files.Open(a.Key)
			if err != nil {
				return j, err
			}
			var report struct {
				Summary domain.Summary `json:"summary"`
			}
			err = json.NewDecoder(io.LimitReader(file, 1<<20)).Decode(&report)
			file.Close()
			if err != nil || report.Summary != sum {
				return j, invalid("summary artifact differs from completion")
			}
		}
	}
	return s.Repo.Complete(ctx, r, sum, artifacts)
}
