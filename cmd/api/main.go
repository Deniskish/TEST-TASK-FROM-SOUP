package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"soup-job-queue/internal/api"
	"soup-job-queue/internal/config"
	"soup-job-queue/internal/storage"
	"soup-job-queue/internal/telemetry"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		c := http.Client{Timeout: 2 * time.Second}
		r, e := c.Get("http://127.0.0.1:8080/health")
		if e != nil {
			return errors.New("healthcheck failed")
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			return errors.New("not ready")
		}
		return nil
	}
	c, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer telemetry.Init(ctx, "soup-api")()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	s, err := storage.Open(startup, c.DatabaseURL, c.PoolSize)
	if err != nil {
		return err
	}
	defer s.Pool.Close()
	telemetry.Serve(ctx, s.Pool, true)
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if s.Migrate(startup) != nil {
			return errors.New("migration failed")
		}
		return nil
	}
	if c.Token == "" {
		slog.Warn("API authentication disabled; use loopback only")
	}
	server := &http.Server{Addr: c.Addr, Handler: telemetry.HTTP((&api.API{Store: s, Token: c.Token}).Handler()), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 6 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	slog.Info("API started", "address", c.Addr)
	select {
	case err = <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return errors.New("HTTP server failed")
		}
		return nil
	case <-ctx.Done():
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancelShutdown()
	if err = server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		return errors.New("HTTP shutdown timed out")
	}
	return nil
}
