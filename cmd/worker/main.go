package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"soup-job-queue/internal/config"
	"soup-job-queue/internal/storage"
	"soup-job-queue/internal/telemetry"
	"soup-job-queue/internal/worker"
	"syscall"
	"time"
)

func main() {
	c, err := config.Load()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer telemetry.Init(ctx, "soup-worker")()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	s, err := storage.Open(startup, c.DatabaseURL, c.PoolSize)
	cancel()
	if err != nil {
		slog.Error("database unavailable at startup")
		os.Exit(1)
	}
	defer s.Pool.Close()
	telemetry.Serve(ctx, s.Pool, false)
	worker.Run(ctx, s, c)
}
