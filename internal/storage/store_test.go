package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"soup-job-queue/internal/jobs"
)

// This suite truncates tables and must only run in an explicitly disposable test DB.
func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set: integration test not run")
	}
	if os.Getenv("ALLOW_TEST_TRUNCATE") != "yes" {
		t.Fatal("ALLOW_TEST_TRUNCATE=yes required")
	}
	ctx := context.Background()
	s, e := Open(ctx, url, 24)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Pool.Close)
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Pool.Exec(ctx, "TRUNCATE outbox,job_effects,jobs RESTART IDENTITY"); e != nil {
		t.Fatal(e)
	}
	return s
}
func create(t *testing.T, s *Store, key string, max int) jobs.Job {
	t.Helper()
	j, _, e := s.Create(context.Background(), key, jobs.Request{Model: "test", MaxAttempts: max})
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func claim(t *testing.T, s *Store) jobs.Job {
	t.Helper()
	j, e := s.Claim(context.Background(), time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func invariant(t *testing.T, s *Store) {
	t.Helper()
	var n int
	e := s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id WHERE (j.status='succeeded')<>(e.job_id IS NOT NULL) OR (e.job_id IS NOT NULL AND e.token<>j.token)`).Scan(&n)
	if e != nil || n != 0 {
		t.Fatalf("invariant violations=%d err=%v", n, e)
	}
}
func TestDoubleClaim(t *testing.T) {
	s := testStore(t)
	create(t, s, "double", 5)
	start := make(chan struct{})
	out := make(chan error, 16)
	for range 16 {
		go func() { <-start; _, e := s.Claim(context.Background(), time.Minute); out <- e }()
	}
	close(start)
	won := 0
	for range 16 {
		e := <-out
		if e == nil {
			won++
		} else if !errors.Is(e, pgx.ErrNoRows) {
			t.Fatal(e)
		}
	}
	if won != 1 {
		t.Fatalf("claims=%d", won)
	}
}
func TestConcurrentDuplicate(t *testing.T) {
	s := testStore(t)
	start := make(chan struct{})
	ids := make(chan int64, 24)
	errs := make(chan error, 24)
	for range 24 {
		go func() {
			<-start
			j, _, e := s.Create(context.Background(), "same", jobs.Request{Model: "x", MaxAttempts: 5})
			ids <- j.ID
			errs <- e
		}()
	}
	close(start)
	var first int64
	for range 24 {
		id := <-ids
		if first == 0 {
			first = id
		}
		if id != first {
			t.Fatal("duplicate id")
		}
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
	}
	_, _, e := s.Create(context.Background(), "same", jobs.Request{Model: "different", MaxAttempts: 5})
	if !errors.Is(e, jobs.ErrConflict) {
		t.Fatal(e)
	}
}
func TestZombieAndExhaustion(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "zombie", 2)
	old := claim(t, s)
	_, e := s.Pool.Exec(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", old.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Heartbeat(ctx, old, time.Minute); !errors.Is(e, jobs.ErrLostLease) {
		t.Fatalf("expired renewal: %v", e)
	}
	if e = s.Finish(ctx, old, false); !errors.Is(e, jobs.ErrLostLease) {
		t.Fatalf("expired finish: %v", e)
	}
	fresh := claim(t, s)
	if fresh.Token <= old.Token {
		t.Fatal("token not advanced")
	}
	if e = s.Finish(ctx, old, false); !errors.Is(e, jobs.ErrLostLease) {
		t.Fatalf("stale finish: %v", e)
	}
	if e = s.Finish(ctx, fresh, false); e != nil {
		t.Fatal(e)
	}
	invariant(t, s)
	create(t, s, "exhausted", 1)
	last := claim(t, s)
	_, e = s.Pool.Exec(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", last.ID)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Claim(ctx, time.Minute)
	if !errors.Is(e, pgx.ErrNoRows) {
		t.Fatal(e)
	}
	j, e := s.Get(ctx, last.ID)
	if e != nil || j.Status != "failed" {
		t.Fatalf("not failed %+v %v", j, e)
	}
	invariant(t, s)
}
func TestCancelCompleteRace(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	counts := map[string]int{}
	for i := range 100 {
		create(t, s, fmt.Sprint(i), 5)
		j := claim(t, s)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		errs := make(chan error, 2)
		go func() { defer wg.Done(); <-start; errs <- s.Finish(ctx, j, false) }()
		go func() { defer wg.Done(); <-start; _, e := s.Cancel(ctx, j.ID); errs <- e }()
		close(start)
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil && !errors.Is(e, jobs.ErrLostLease) && !errors.Is(e, jobs.ErrTerminal) {
				t.Fatal(e)
			}
		}
		got, e := s.Get(ctx, j.ID)
		if e != nil {
			t.Fatal(e)
		}
		counts[got.Status]++
		invariant(t, s)
	}
	t.Logf("race outcomes: %v", counts)
}
func TestAtomicRollback(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "rollback", 5)
	j := claim(t, s)
	// Fault injection at the second write proves that the earlier status update is rolled back.
	_, e := s.Pool.Exec(ctx, `CREATE FUNCTION reject_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected effect failure'; END $$; CREATE TRIGGER reject_effect BEFORE INSERT ON job_effects FOR EACH ROW EXECUTE FUNCTION reject_effect()`)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, _ = s.Pool.Exec(context.Background(), "DROP TRIGGER reject_effect ON job_effects; DROP FUNCTION reject_effect()")
	})
	if e = s.Finish(ctx, j, false); e == nil {
		t.Fatal("injected fault ignored")
	}
	got, e := s.Get(ctx, j.ID)
	if e != nil || got.Status != "running" {
		t.Fatalf("partial transaction %+v %v", got, e)
	}
	invariant(t, s)
}
func TestReadCommittedAndLockWaitExpiry(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "isolation", 5)
	j, e := s.Claim(ctx, 400*time.Millisecond)
	if e != nil {
		t.Fatal(e)
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	var isolation string
	if e = tx.QueryRow(ctx, "SHOW transaction_isolation").Scan(&isolation); e != nil || isolation != "read committed" {
		t.Fatalf("%s %v", isolation, e)
	}
	if e = lockJob(ctx, tx, j.ID); e != nil {
		t.Fatal(e)
	}
	result := make(chan error, 1)
	go func() { result <- s.Finish(ctx, j, false) }()
	// This deliberate lock hold crosses lease expiry. It catches time checks made before locking.
	time.Sleep(600 * time.Millisecond)
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-result; !errors.Is(e, jobs.ErrLostLease) {
		t.Fatalf("finish after lock wait: %v", e)
	}
	invariant(t, s)
}
func TestRetryBudgetAndCancelOrders(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "retry", 2)
	j := claim(t, s)
	if e := s.Finish(ctx, j, true); e != nil {
		t.Fatal(e)
	}
	j = claim(t, s)
	if e := s.Finish(ctx, j, true); e != nil {
		t.Fatal(e)
	}
	got, _ := s.Get(ctx, j.ID)
	if got.Status != "failed" {
		t.Fatal(got)
	}
	create(t, s, "cancel-first", 5)
	j = claim(t, s)
	if _, e := s.Cancel(ctx, j.ID); e != nil {
		t.Fatal(e)
	}
	if e := s.Finish(ctx, j, false); !errors.Is(e, jobs.ErrLostLease) {
		t.Fatal(e)
	}
	create(t, s, "finish-first", 5)
	j = claim(t, s)
	if e := s.Finish(ctx, j, false); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Cancel(ctx, j.ID); !errors.Is(e, jobs.ErrTerminal) {
		t.Fatal(e)
	}
	invariant(t, s)
}

func TestOutboxAtomicity(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := create(t, s, "outbox", 5)
	var n int
	if e := s.Pool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE job_id=$1 AND published_at IS NULL", j.ID).Scan(&n); e != nil || n != 1 {
		t.Fatalf("missing event %d %v", n, e)
	}
	_, e := s.Pool.Exec(ctx, `CREATE FUNCTION reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox fault'; END $$; CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_outbox()`)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, _ = s.Pool.Exec(context.Background(), "DROP TRIGGER reject_outbox ON outbox; DROP FUNCTION reject_outbox()")
	})
	if _, _, e = s.Create(ctx, "must-rollback", jobs.Request{Model: "x", MaxAttempts: 5}); e == nil {
		t.Fatal("injected outbox error ignored")
	}
	if e = s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE idempotency_key='must-rollback'").Scan(&n); e != nil || n != 0 {
		t.Fatalf("job committed without outbox %d %v", n, e)
	}
	// The database harness runs without workers and checks terminal states afterwards.
	if _, e = s.Cancel(ctx, j.ID); e != nil {
		t.Fatal(e)
	}
}
