package transport

import (
	"context"
	"errors"
	"io"
	"time"

	pb "github.com/helioLJ/csvcheck/api/csvcheck/v1"
	"github.com/helioLJ/csvcheck/internal/coordinator"
	"github.com/helioLJ/csvcheck/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedValidationServiceServer
	pb.UnimplementedWorkerServiceServer
	Service *coordinator.Service
}
type uploadReader struct {
	stream  grpc.ClientStreamingServer[pb.UploadFileRequest, pb.File]
	pending []byte
}

func (r *uploadReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		msg, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		chunk, ok := msg.Payload.(*pb.UploadFileRequest_Chunk)
		if !ok {
			return 0, status.Error(codes.InvalidArgument, "metadata must appear exactly once, before chunks")
		}
		if len(chunk.Chunk) > 64<<10 {
			return 0, status.Error(codes.InvalidArgument, "upload chunks cannot exceed 64 KiB")
		}
		r.pending = chunk.Chunk
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
func (s *Server) UploadFile(stream grpc.ClientStreamingServer[pb.UploadFileRequest, pb.File]) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "upload metadata is required")
	}
	metadata, ok := first.Payload.(*pb.UploadFileRequest_Metadata)
	if !ok {
		return status.Error(codes.InvalidArgument, "first message must contain metadata")
	}
	f, err := s.Service.Upload(stream.Context(), metadata.Metadata.Filename, &uploadReader{stream: stream})
	if err != nil {
		return rpcError(err)
	}
	return stream.SendAndClose(&pb.File{Id: f.ID, Filename: f.Filename, SizeBytes: f.Size, Sha256: f.SHA256})
}
func (s *Server) SubmitJob(ctx context.Context, r *pb.SubmitJobRequest) (*pb.Job, error) {
	j, err := s.Service.Submit(ctx, r.IdempotencyKey, r.FileId, Config(r.Config))
	return job(j), rpcError(err)
}
func (s *Server) GetJob(ctx context.Context, r *pb.GetJobRequest) (*pb.Job, error) {
	j, err := s.Service.Repo.Get(ctx, r.JobId)
	return job(j), rpcError(err)
}
func (s *Server) CancelJob(ctx context.Context, r *pb.GetJobRequest) (*pb.Job, error) {
	j, err := s.Service.Repo.Cancel(ctx, r.JobId)
	return job(j), rpcError(err)
}
func (s *Server) WatchJob(r *pb.WatchJobRequest, stream grpc.ServerStreamingServer[pb.JobEvent]) error {
	if r.AfterSequence < 0 {
		return status.Error(codes.InvalidArgument, "sequence cannot be negative")
	}
	current, err := s.Service.Repo.Get(stream.Context(), r.JobId)
	if err != nil {
		return rpcError(err)
	}
	if r.AfterSequence > current.Sequence {
		return status.Error(codes.InvalidArgument, "sequence is ahead of the job")
	}
	after := r.AfterSequence
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, err := s.Service.Repo.Events(stream.Context(), r.JobId, after)
		if err != nil {
			return rpcError(err)
		}
		for _, e := range events {
			if err = stream.Send(&pb.JobEvent{Sequence: e.Sequence, Kind: e.Kind, Job: job(e.Job)}); err != nil {
				return err
			}
			after = e.Sequence
			if e.Job.Terminal() {
				return nil
			}
		}
		if len(events) == 100 {
			continue
		}
		j, err := s.Service.Repo.Get(stream.Context(), r.JobId)
		if err != nil {
			return rpcError(err)
		}
		if j.Terminal() && after >= j.Sequence {
			return nil
		}
		select {
		case <-stream.Context().Done():
			return rpcError(stream.Context().Err())
		case <-ticker.C:
		}
	}
}
func (s *Server) DownloadArtifact(r *pb.DownloadArtifactRequest, stream grpc.ServerStreamingServer[pb.ArtifactChunk]) error {
	a, err := s.Service.Repo.Artifact(stream.Context(), r.JobId, r.Kind)
	if err != nil {
		return rpcError(err)
	}
	f, err := s.Service.Files.Open(a.Key)
	if err != nil {
		return rpcError(err)
	}
	defer f.Close()
	buf := make([]byte, 64<<10)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.ArtifactChunk{Data: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return rpcError(err)
		}
	}
}
func (s *Server) ClaimJob(ctx context.Context, r *pb.ClaimJobRequest) (*pb.ClaimJobResponse, error) {
	if len(r.WorkerId) < 1 || len(r.WorkerId) > 128 {
		return nil, status.Error(codes.InvalidArgument, "worker ID must contain 1 to 128 characters")
	}
	a, err := s.Service.Repo.Claim(ctx, r.WorkerId)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &pb.ClaimJobResponse{}
	if a != nil {
		response.Assignment = &pb.Assignment{JobId: a.JobID, AttemptId: a.AttemptID, LeaseToken: a.Token, LeaseExpiresUnix: a.Expires.Unix(), InputKey: a.InputKey, InputSize: a.InputSize, Config: ProtoConfig(a.Config), OutputPrefix: a.OutputPrefix}
	}
	return response, nil
}
func (s *Server) Heartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	p := domain.Progress{BytesConsumed: r.GetProgress().GetBytesConsumed(), RecordsProcessed: r.GetProgress().GetRecordsProcessed()}
	expires, err := s.Service.Repo.Heartbeat(ctx, ref(r.Attempt), p)
	if errors.Is(err, domain.ErrStale) {
		return &pb.HeartbeatResponse{CancelRequested: true}, nil
	}
	return &pb.HeartbeatResponse{LeaseExpiresUnix: expires.Unix()}, rpcError(err)
}
func (s *Server) CompleteAttempt(ctx context.Context, r *pb.CompleteAttemptRequest) (*pb.Job, error) {
	var artifacts []domain.Artifact
	for _, a := range r.Artifacts {
		artifacts = append(artifacts, domain.Artifact{Kind: a.Kind, Key: a.Key, Size: a.SizeBytes, SHA256: a.Sha256})
	}
	j, err := s.Service.Complete(ctx, ref(r.Attempt), Summary(r.Summary), artifacts)
	return job(j), rpcError(err)
}
func (s *Server) FailAttempt(ctx context.Context, r *pb.FailAttemptRequest) (*pb.Job, error) {
	if r.Code != "INVALID_CSV" && r.Code != "IO_ERROR" {
		return nil, status.Error(codes.InvalidArgument, "unknown failure code")
	}
	if len(r.Message) > 500 {
		return nil, status.Error(codes.InvalidArgument, "failure message exceeds 500 bytes")
	}
	j, err := s.Service.Repo.Fail(ctx, ref(r.Attempt), r.Code, r.Message)
	return job(j), rpcError(err)
}
