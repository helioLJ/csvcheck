package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/helioLJ/csvcheck/api/csvcheck/v1"
	"github.com/helioLJ/csvcheck/internal/config"
	"github.com/helioLJ/csvcheck/internal/coordinator"
	"github.com/helioLJ/csvcheck/internal/postgres"
	"github.com/helioLJ/csvcheck/internal/storage"
	"github.com/helioLJ/csvcheck/internal/transport"
	"google.golang.org/grpc"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("coordinator stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	lease, err := config.Duration("LEASE_DURATION", 15*time.Second)
	if err != nil {
		return err
	}
	if lease < 3*time.Second {
		return errors.New("lease must be at least 3s")
	}
	clientToken, workerToken := os.Getenv("CLIENT_TOKEN"), os.Getenv("WORKER_TOKEN")
	if len(clientToken) < 12 || len(workerToken) < 12 || clientToken == workerToken {
		return errors.New("set distinct CLIENT_TOKEN and WORKER_TOKEN values of at least 12 characters")
	}
	db, err := postgres.New(ctx, os.Getenv("DATABASE_URL"), lease)
	if err != nil {
		return err
	}
	defer db.Pool.Close()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		return db.Migrate(ctx)
	}
	files, err := storage.New(config.Env("DATA_DIR", "./data"))
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.Env("LISTEN_ADDR", "127.0.0.1:50051"))
	if err != nil {
		return err
	}
	defer listener.Close()
	server := grpc.NewServer(grpc.MaxRecvMsgSize(2<<20), grpc.MaxSendMsgSize(2<<20), grpc.MaxConcurrentStreams(64), grpc.UnaryInterceptor(transport.UnaryAuth(clientToken, workerToken)), grpc.StreamInterceptor(transport.StreamAuth(clientToken, workerToken)))
	handler := &transport.Server{Service: &coordinator.Service{Repo: db, Files: files}}
	pb.RegisterValidationServiceServer(server, handler)
	pb.RegisterWorkerServiceServer(server, handler)
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := db.Reap(ctx); err != nil && ctx.Err() == nil {
					slog.Error("lease recovery failed", "error", err)
				}
			}
		}
	}()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	slog.Info("coordinator ready", "address", listener.Addr().String())
	select {
	case err := <-done:
		stop()
		<-reaperDone
		return err
	case <-ctx.Done():
	}
	stopped := make(chan struct{})
	go func() { server.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		server.Stop()
		<-stopped
	}
	<-reaperDone
	return nil
}
