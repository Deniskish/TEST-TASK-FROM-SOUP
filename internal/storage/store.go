package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"soup-job-queue/internal/jobs"
	"soup-job-queue/internal/telemetry"
	"soup-job-queue/migrations"
)

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, url string, maxConns int32) (*Store, error) {
	c, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	c.MaxConns = maxConns
	c.ConnConfig.ConnectTimeout = 3 * time.Second
	c.ConnConfig.RuntimeParams["statement_timeout"] = "4000"
	c.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	// Explicit even when a database administrator changed the server default.
	c.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, errors.New("cannot create database pool")
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, errors.New("database unavailable")
	}
	return &Store{p}, nil
}

// Cleanup must not inherit a cancelled request, but must also not wait forever
// on a partitioned connection. pgx closes an unusable connection on rollback error.
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	// Serializes simultaneous migrators. Version 1 is immutable; later changes need a new migration.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(54821936)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, migrations.Schema+migrations.Outbox); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

const columns = "id,payload,status,attempts,max_attempts,token,lease_until,created_at,updated_at,last_error"

func scan(row pgx.Row) (jobs.Job, error) {
	var j jobs.Job
	var payload []byte
	err := row.Scan(&j.ID, &payload, &j.Status, &j.Attempts, &j.MaxAttempts, &j.Token, &j.LeaseUntil, &j.CreatedAt, &j.UpdatedAt, &j.LastError)
	if err == nil {
		err = json.Unmarshal(payload, &j.Payload)
	}
	return j, err
}

func (s *Store) Create(ctx context.Context, key string, r jobs.Request) (jobs.Job, bool, error) {
	ctx, span := telemetry.Start(ctx, "postgres.Create")
	defer span.End()

	payload, err := json.Marshal(r)
	if err != nil {
		return jobs.Job{}, false, err
	}
	j, err := scan(s.Pool.QueryRow(ctx, `INSERT INTO jobs(idempotency_key,payload,max_attempts,trace_context) VALUES($1,$2,$3,$4)
 ON CONFLICT(idempotency_key) DO NOTHING RETURNING `+columns, key, payload, r.MaxAttempts, telemetry.CarrierJSON(ctx)))
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return j, false, err
	}
	// Separate statement gets a fresh READ COMMITTED snapshot after waiting on the unique key.
	// A single INSERT/SELECT CTE can miss the conflicting row from its older snapshot.
	j, err = scan(s.Pool.QueryRow(ctx, "SELECT "+columns+" FROM jobs WHERE idempotency_key=$1", key))
	if err == nil && j.Payload != r {
		return j, false, jobs.ErrConflict
	}
	return j, false, err
}
func (s *Store) Get(ctx context.Context, id int64) (jobs.Job, error) {
	return scan(s.Pool.QueryRow(ctx, "SELECT "+columns+" FROM jobs WHERE id=$1", id))
}
func (s *Store) List(ctx context.Context, after int64, limit int) ([]jobs.Job, error) {
	rows, err := s.Pool.Query(ctx, "SELECT "+columns+" FROM jobs WHERE id>$1 ORDER BY id LIMIT $2", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]jobs.Job, 0, limit)
	for rows.Next() {
		j, e := scan(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
func (s *Store) Cancel(ctx context.Context, id int64) (jobs.Job, error) {
	ctx, span := telemetry.Start(ctx, "postgres.Cancel")
	defer span.End()

	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return jobs.Job{}, err
	}
	defer rollback(tx)
	j, err := scan(tx.QueryRow(ctx, "SELECT "+columns+" FROM jobs WHERE id=$1 FOR UPDATE", id))
	if err != nil {
		return j, err
	}
	if j.Status == "succeeded" || j.Status == "failed" {
		return j, jobs.ErrTerminal
	}
	if j.Status != "cancelled" {
		j, err = scan(tx.QueryRow(ctx, `UPDATE jobs SET status='cancelled',token=token+1,lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1 RETURNING `+columns, id))
		if err != nil {
			return j, err
		}
	}
	return j, tx.Commit(ctx)
}

func (s *Store) Claim(ctx context.Context, lease time.Duration) (jobs.Job, error) {
	return s.ClaimID(ctx, lease, 0)
}
func (s *Store) ClaimID(ctx context.Context, lease time.Duration, target int64) (jobs.Job, error) {
	ctx, span := telemetry.Start(ctx, "postgres.ClaimID")
	defer span.End()

	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return jobs.Job{}, err
	}
	defer rollback(tx)
	// Every worker is also a bounded reaper: even the final crashed attempt eventually terminates.
	reaped, err := tx.Exec(ctx, `WITH expired AS (
 SELECT id FROM jobs WHERE status='running' AND lease_until<=clock_timestamp() AND attempts>=max_attempts
 ORDER BY lease_until LIMIT 100 FOR UPDATE SKIP LOCKED)
 UPDATE jobs j SET status='failed',lease_until=NULL,token=token+1,last_error='attempt budget exhausted after lease expiry',updated_at=clock_timestamp()
 FROM expired e WHERE j.id=e.id`)
	if err != nil {
		return jobs.Job{}, err
	}
	var id int64
	var expired bool
	err = tx.QueryRow(ctx, `SELECT id,(status='running') FROM jobs WHERE (status='queued' OR (status='running' AND lease_until<=clock_timestamp()))
 AND attempts<max_attempts AND ($1::bigint=0 OR id=$1) ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`, target).Scan(&id, &expired)
	if errors.Is(err, pgx.ErrNoRows) {
		if e := tx.Commit(ctx); e != nil {
			return jobs.Job{}, e
		}
		telemetry.Expired.Add(float64(reaped.RowsAffected()))
		return jobs.Job{}, pgx.ErrNoRows
	}
	if err != nil {
		return jobs.Job{}, err
	}
	j, err := scan(tx.QueryRow(ctx, `UPDATE jobs SET status='running',attempts=attempts+1,token=token+1,
 lease_until=clock_timestamp()+$2*interval '1 millisecond',updated_at=clock_timestamp() WHERE id=$1 RETURNING `+columns, id, lease.Milliseconds()))
	if err != nil {
		return j, err
	}
	err = tx.Commit(ctx)
	if err == nil {
		n := reaped.RowsAffected()
		if expired {
			n++
		}
		telemetry.Expired.Add(float64(n))
	}
	return j, err
}

func lockJob(ctx context.Context, tx pgx.Tx, id int64) error {
	var found int64
	return tx.QueryRow(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", id).Scan(&found)
}

func (s *Store) Heartbeat(ctx context.Context, j jobs.Job, lease time.Duration) error {
	ctx, span := telemetry.Start(ctx, "postgres.Heartbeat")
	defer span.End()

	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err = lockJob(ctx, tx, j.ID); err != nil {
		return err
	}
	// Check time in a new statement AFTER the lock, so lock waiting cannot renew an expired lease.
	tag, err := tx.Exec(ctx, `UPDATE jobs SET lease_until=clock_timestamp()+$3*interval '1 millisecond',updated_at=clock_timestamp()
 WHERE id=$1 AND token=$2 AND status='running' AND lease_until>clock_timestamp()`, j.ID, j.Token, lease.Milliseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		telemetry.Stale.Inc()
		return jobs.ErrLostLease
	}
	return tx.Commit(ctx)
}

func (s *Store) Finish(ctx context.Context, j jobs.Job, failed bool) error {
	ctx, span := telemetry.Start(ctx, "postgres.Finish")
	defer span.End()

	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err = lockJob(ctx, tx, j.ID); err != nil {
		return err
	}
	status := "succeeded"
	message := ""
	if failed {
		status = "queued"
		message = "simulated training failure"
		if j.Attempts >= j.MaxAttempts {
			status = "failed"
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status=$3,last_error=$4,lease_until=NULL,updated_at=clock_timestamp()
 WHERE id=$1 AND token=$2 AND status='running' AND lease_until>clock_timestamp()`, j.ID, j.Token, status, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		telemetry.Stale.Inc()
		return jobs.ErrLostLease
	}
	if !failed {
		// Intentionally no ON CONFLICT: an existing effect here indicates broken invariants.
		// Both writes roll back on any failure, including the unique constraint failure.
		_, err = tx.Exec(ctx, `INSERT INTO job_effects(job_id,token,result) VALUES($1,$2,jsonb_build_object('model',$3::text))`, j.ID, j.Token, j.Payload.Model)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
