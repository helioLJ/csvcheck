package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/helioLJ/csvcheck/api/csvcheck/v1"
	"github.com/helioLJ/csvcheck/internal/config"
	"github.com/helioLJ/csvcheck/internal/domain"
	"github.com/helioLJ/csvcheck/internal/storage"
	"github.com/helioLJ/csvcheck/internal/transport"
	"github.com/helioLJ/csvcheck/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	token := os.Getenv("WORKER_TOKEN")
	if len(token) < 12 {
		return errors.New("set WORKER_TOKEN to at least 12 characters")
	}
	ctx = transport.Outgoing(ctx, token)
	conn, err := grpc.NewClient(config.Env("COORDINATOR_ADDR", "localhost:50051"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	files, err := storage.New(config.Env("DATA_DIR", "./data"))
	if err != nil {
		return err
	}
	w := worker.Worker{Client: pb.NewWorkerServiceClient(conn), Files: files, ID: config.Env("WORKER_ID", domain.ID()), HeartbeatInterval: time.Second}
	return w.Run(ctx)
}
