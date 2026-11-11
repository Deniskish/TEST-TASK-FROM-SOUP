package main

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"log/slog"
	"os"
	"os/signal"
	"soup-job-queue/internal/broker"
	"soup-job-queue/internal/config"
	"soup-job-queue/internal/storage"
	"soup-job-queue/internal/telemetry"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer telemetry.Init(ctx, "soup-publisher")()
	c, e := config.Load()
	if e != nil {
		panic(e)
	}
	s, e := storage.Open(ctx, c.DatabaseURL, c.PoolSize)
	if e != nil {
		panic("database unavailable")
	}
	defer s.Pool.Close()
	telemetry.Serve(ctx, s.Pool, false)
	var b *broker.Session
	defer func() {
		if b != nil {
			b.Close()
		}
	}()
	next := time.Now()
	for ctx.Err() == nil {
		e = nil
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		if time.Now().After(next) {
			if e = s.Reconcile(op); e != nil {
				slog.Warn("reconciliation unavailable")
			}
			next = time.Now().Add(5 * time.Second)
		}
		if b == nil {
			b, e = broker.Open()
		}
		if e == nil {
			e = s.PublishOne(op, b)
		}
		cancel()
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			if b != nil {
				b.Close()
				b = nil
			}
			slog.Warn("publication unavailable")
		}
		delay := time.Millisecond
		if e != nil {
			delay = 200 * time.Millisecond
		}
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
	}
}
