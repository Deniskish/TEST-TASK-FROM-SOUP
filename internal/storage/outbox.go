package storage

import (
	"context"
	"encoding/json"
	"soup-job-queue/internal/broker"
	"soup-job-queue/internal/telemetry"
)

// Lock remains held until confirm. Bounded context limits the publication transaction;
// a crash between confirm and commit deliberately permits duplicate delivery.
func (s *Store) PublishOne(ctx context.Context, b *broker.Session) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer rollback(tx)
	var id int64
	var carrier []byte
	e = tx.QueryRow(ctx, "SELECT job_id,trace_context FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED").Scan(&id, &carrier)
	if e != nil {
		return e
	}
	ctx = telemetry.Extract(ctx, carrier)
	ctx, span := telemetry.Start(ctx, "outbox.publish")
	defer span.End()
	body, _ := json.Marshal(map[string]int64{"job_id": id})
	if e = b.Publish(ctx, body); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE outbox SET published_at=clock_timestamp() WHERE job_id=$1", id); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Reconcile(ctx context.Context) error {
	// Existing rows are reset only after a grace period, limiting duplicate traffic.
	_, e := s.Pool.Exec(ctx, `INSERT INTO outbox(job_id,trace_context)
 SELECT id,trace_context FROM jobs WHERE status='queued' OR (status='running' AND lease_until<=clock_timestamp())
 ON CONFLICT(job_id) DO UPDATE SET published_at=NULL,created_at=clock_timestamp()
 WHERE outbox.published_at<clock_timestamp()-interval '30 seconds'`)
	return e
}
