package worker

import (
	"context"
	"encoding/json"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
	"os"
	"soup-job-queue/internal/broker"
	"soup-job-queue/internal/telemetry"
	"time"

	"github.com/jackc/pgx/v5"
	"soup-job-queue/internal/config"
	"soup-job-queue/internal/jobs"
	"soup-job-queue/internal/storage"
)

// One process handles one job at a time. Compose supplies independent processes.
// Timers measure durations; wall clock is never consulted to decide lease validity.
func Run(ctx context.Context, s *storage.Store, c config.Config) {
	for ctx.Err() == nil {
		b, err := broker.Open()
		if err != nil {
			wait(ctx, time.Second)
			continue
		}
		_ = b.Channel.Qos(1, 0, false)
		deliveries, err := b.Channel.Consume(broker.Queue, "", false, false, false, false, nil)
		if err == nil {
			consume(ctx, s, c, deliveries)
		}
		b.Close()
		wait(ctx, time.Second)
	}
}
func consume(ctx context.Context, s *storage.Store, c config.Config, ds <-chan amqp.Delivery) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-ds:
			if !ok {
				return
			}
			var msg struct {
				ID int64 `json:"job_id"`
			}
			if json.Unmarshal(d.Body, &msg) != nil || msg.ID <= 0 {
				_ = d.Reject(false)
				continue
			}
			carrier := map[string]string{}
			for _, k := range []string{"traceparent", "tracestate"} {
				if v, ok := d.Headers[k].(string); ok {
					carrier[k] = v
				}
			}
			raw, _ := json.Marshal(carrier)
			op, span := telemetry.Start(telemetry.Extract(ctx, raw), "worker.process")
			deadline, cancel := context.WithTimeout(op, 5*time.Second)
			j, err := s.ClaimID(deadline, c.Lease, msg.ID)
			cancel()
			if errors.Is(err, pgx.ErrNoRows) {
				deadline, cancel = context.WithTimeout(op, 5*time.Second)
				existing, e := s.Get(deadline, msg.ID)
				cancel()
				if e == nil && (existing.Status == "succeeded" || existing.Status == "failed" || existing.Status == "cancelled") {
					_ = d.Ack(false)
				} else if e == nil || errors.Is(e, pgx.ErrNoRows) {
					_ = d.Reject(false)
				} else {
					span.End()
					return
				}
				span.End()
				continue
			}
			if err != nil {
				span.End()
				return
			}
			if j.Attempts > 1 {
				telemetry.Retries.Inc()
			}
			slog.Info("claimed", "job_id", j.ID, "token", j.Token, "attempt", j.Attempts, "process_wall_time", time.Now().Add(c.ClockOffset))
			start := time.Now()
			err = execute(op, s, c, j)
			telemetry.Processing.Observe(time.Since(start).Seconds())
			span.End()
			if err != nil {
				return
			}
			// Test-only pause allows a real crash in the commit-to-ACK window.
			if pause, _ := time.ParseDuration(os.Getenv("TEST_BEFORE_ACK_DELAY")); pause > 0 {
				wait(ctx, pause)
			}
			if ctx.Err() != nil {
				return
			}
			if d.Ack(false) != nil {
				return
			}
		}
	}
}

func execute(ctx context.Context, s *storage.Store, c config.Config, j jobs.Job) error {
	timer := time.NewTimer(time.Duration(j.Payload.DurationMS) * time.Millisecond)
	defer timer.Stop()
	heartbeat := time.NewTicker(c.Heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // Leave lease for recovery: no unsafe release after uncertain DB outcomes.
		case <-heartbeat.C:
			op, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := s.Heartbeat(op, j, c.Lease)
			cancel()
			if err != nil {
				slog.Warn("ownership lost or heartbeat unavailable", "job_id", j.ID, "token", j.Token)
				return err
			}
		case <-timer.C:
			op, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := s.Finish(op, j, j.Attempts <= j.Payload.FailUntilAttempt)
			cancel()
			if err != nil {
				slog.Warn("finish rejected or outcome unknown", "job_id", j.ID, "token", j.Token)
			} else {
				slog.Info("attempt committed", "job_id", j.ID, "token", j.Token)
			}
			return err
		}
	}
}
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
