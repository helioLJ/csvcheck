// Package transport maps gRPC messages and errors to application types.
package transport

import (
	"context"
	"errors"
	"log/slog"
	"time"

	pb "github.com/helioLJ/csvcheck/api/csvcheck/v1"
	"github.com/helioLJ/csvcheck/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Config(p *pb.ValidationConfig) domain.Config {
	if p == nil {
		return domain.Config{}
	}
	c := domain.Config{Delimiter: p.Delimiter}
	for _, r := range p.Rules {
		d := domain.Rule{Column: r.Column, RequiredColumn: r.RequiredColumn, RequiredValue: r.RequiredValue}
		if v := r.GetDecimal(); v != nil {
			d.Decimal = &domain.Decimal{Minimum: v.Minimum, Maximum: v.Maximum, MinimumExclusive: v.MinimumExclusive, MaximumExclusive: v.MaximumExclusive}
		}
		if v := r.GetDate(); v != nil {
			d.Date = &domain.Date{Format: v.Format}
		}
		if v := r.GetText(); v != nil {
			d.Text = &domain.Text{MinLength: v.MinLength, MaxLength: v.MaxLength}
		}
		c.Rules = append(c.Rules, d)
	}
	return c
}
func ProtoConfig(c domain.Config) *pb.ValidationConfig {
	p := &pb.ValidationConfig{Delimiter: c.Delimiter}
	for _, r := range c.Rules {
		v := &pb.ColumnRule{Column: r.Column, RequiredColumn: r.RequiredColumn, RequiredValue: r.RequiredValue}
		if d := r.Decimal; d != nil {
			v.Constraint = &pb.ColumnRule_Decimal{Decimal: &pb.DecimalConstraint{Minimum: d.Minimum, Maximum: d.Maximum, MinimumExclusive: d.MinimumExclusive, MaximumExclusive: d.MaximumExclusive}}
		}
		if d := r.Date; d != nil {
			v.Constraint = &pb.ColumnRule_Date{Date: &pb.DateConstraint{Format: d.Format}}
		}
		if d := r.Text; d != nil {
			v.Constraint = &pb.ColumnRule_Text{Text: &pb.TextConstraint{MinLength: d.MinLength, MaxLength: d.MaxLength}}
		}
		p.Rules = append(p.Rules, v)
	}
	return p
}
func job(j domain.Job) *pb.Job {
	p := &pb.Job{Id: j.ID, State: j.State, ValidationOutcome: j.Outcome, FileId: j.FileID, Config: ProtoConfig(j.Config), AttemptCount: int32(j.AttemptCount), MaxAttempts: int32(j.MaxAttempts), CurrentAttempt: j.CurrentAttempt, CancelRequested: j.CancelRequested, Progress: &pb.Progress{BytesConsumed: j.Progress.BytesConsumed, RecordsProcessed: j.Progress.RecordsProcessed}, LastError: j.LastError, LastEventSequence: j.Sequence, CreatedAt: j.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: j.UpdatedAt.Format(time.RFC3339Nano)}
	if j.Summary != nil {
		p.Summary = ProtoSummary(*j.Summary)
	}
	for _, a := range j.Artifacts {
		p.Artifacts = append(p.Artifacts, &pb.PublishedArtifact{Kind: a.Kind, SizeBytes: a.Size, Sha256: a.SHA256})
	}
	return p
}
func ref(r *pb.AttemptRef) domain.AttemptRef {
	if r == nil {
		return domain.AttemptRef{}
	}
	return domain.AttemptRef{JobID: r.JobId, AttemptID: r.AttemptId, Token: r.LeaseToken}
}
func ProtoRef(r domain.AttemptRef) *pb.AttemptRef {
	return &pb.AttemptRef{JobId: r.JobID, AttemptId: r.AttemptID, LeaseToken: r.Token}
}
func Summary(s *pb.Summary) domain.Summary {
	if s == nil {
		return domain.Summary{}
	}
	return domain.Summary{Records: s.Records, InvalidRecords: s.InvalidRecords, Violations: s.Violations, SampledViolations: s.SampledViolations, BytesConsumed: s.BytesConsumed}
}
func ProtoSummary(s domain.Summary) *pb.Summary {
	return &pb.Summary{Records: s.Records, InvalidRecords: s.InvalidRecords, Violations: s.Violations, SampledViolations: s.SampledViolations, BytesConsumed: s.BytesConsumed}
}
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, domain.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrStale):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline expired")
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	slog.Error("request failed", "error", err)
	return status.Error(codes.Internal, "operation failed; inspect coordinator logs")
}
