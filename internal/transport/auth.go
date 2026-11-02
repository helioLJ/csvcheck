package transport

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func authenticate(ctx context.Context, method, clientToken, workerToken string) error {
	expected := clientToken
	if strings.HasPrefix(method, "/csvcheck.v1.WorkerService/") {
		expected = workerToken
	}
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+expected)) != 1 {
		return status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	return nil
}
func UnaryAuth(client, worker string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authenticate(ctx, info.FullMethod, client, worker); err != nil {
			return nil, err
		}
		start := time.Now()
		response, err := handler(ctx, req)
		slog.Info("rpc", "method", info.FullMethod, "duration_ms", time.Since(start).Milliseconds(), "code", status.Code(err).String())
		return response, err
	}
}
func StreamAuth(client, worker string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authenticate(stream.Context(), info.FullMethod, client, worker); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}
func Outgoing(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}
