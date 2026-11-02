package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/helioLJ/csvcheck/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	name := "test_" + domain.ID()
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	cfg.MaxConns = 12
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close(); _, _ = admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE"); admin.Close() })
	s := &Store{Pool: pool, Lease: 15 * time.Second}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}
func seed(t *testing.T, s *Store) domain.Job {
	t.Helper()
	ctx := context.Background()
	f := domain.File{ID: domain.ID(), Filename: "a.csv", Key: domain.ID(), Size: 4, SHA256: "hash"}
	if err := s.AddFile(ctx, f); err != nil {
		t.Fatal(err)
	}
	j, err := s.Submit(ctx, domain.ID(), f.ID, domain.Config{Delimiter: ",", Rules: []domain.Rule{{Column: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func TestConcurrentClaimAndRecovery(t *testing.T) {
	s := testStore(t)
	j := seed(t, s)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan *domain.Assignment, 8)
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			a, err := s.Claim(ctx, domain.ID())
			if err != nil {
				errs <- err
			}
			if a != nil {
				results <- a
			}
		})
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var first *domain.Assignment
	count := 0
	for a := range results {
		first = a
		count++
	}
	if count != 1 {
		t.Fatalf("claims=%d", count)
	}
	if _, err := s.Pool.Exec(ctx, "UPDATE job_attempts SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", first.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := s.Claim(ctx, "replacement")
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || second.AttemptID == first.AttemptID {
		t.Fatal("job not recovered")
	}
	if _, err = s.Heartbeat(ctx, first.AttemptRef, domain.Progress{}); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("stale heartbeat=%v", err)
	}
	current, _ := s.Get(ctx, j.ID)
	if current.AttemptCount != 2 {
		t.Fatal(current)
	}
}
func TestConcurrentSubmitIdempotency(t *testing.T) {
	s := testStore(t)
	j := seed(t, s)
	ctx := context.Background()
	key := domain.ID()
	var wg sync.WaitGroup
	ids := make(chan string, 8)
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			r, e := s.Submit(ctx, key, j.FileID, j.Config)
			if e != nil {
				errs <- e
			} else {
				ids <- r.ID
			}
		})
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	if len(unique) != 1 {
		t.Fatal(unique)
	}
	other := j.Config
	other.Delimiter = ";"
	if _, err := s.Submit(ctx, key, j.FileID, other); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("error=%v", err)
	}
}
func TestCompletionReplayCancelAndEvents(t *testing.T) {
	s := testStore(t)
	j := seed(t, s)
	ctx := context.Background()
	a, err := s.Claim(ctx, "worker")
	if err != nil {
		t.Fatal(err)
	}
	sum := domain.Summary{Records: 1, BytesConsumed: 4}
	completed, err := s.Complete(ctx, a.AttemptRef, sum, nil)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := s.Complete(ctx, a.AttemptRef, sum, nil)
	if err != nil || replay.Sequence != completed.Sequence {
		t.Fatal(replay, err)
	}
	sum.Records = 2
	if _, err = s.Complete(ctx, a.AttemptRef, sum, nil); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("changed replay=%v", err)
	}
	events, err := s.Events(ctx, j.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Sequence != 2 || events[1].Kind != "completed" {
		t.Fatal(events)
	}
	cancelled, err := s.Cancel(ctx, j.ID)
	if err != nil || cancelled.State != "SUCCEEDED" {
		t.Fatal(cancelled, err)
	}
	j = seed(t, s)
	a, err = s.Claim(ctx, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Cancel(ctx, j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Complete(ctx, a.AttemptRef, sum, nil); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("cancelled completion=%v", err)
	}
}
func TestRetriesExhaustAndProgress(t *testing.T) {
	s := testStore(t)
	j := seed(t, s)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		a, err := s.Claim(ctx, "worker")
		if err != nil || a == nil {
			t.Fatal(a, err)
		}
		if _, err = s.Heartbeat(ctx, a.AttemptRef, domain.Progress{BytesConsumed: 5}); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("progress=%v", err)
		}
		if _, err = s.Fail(ctx, a.AttemptRef, "IO_ERROR", "disk unavailable"); err != nil {
			t.Fatal(err)
		}
	}
	final, _ := s.Get(ctx, j.ID)
	if final.State != "FAILED" || final.AttemptCount != 3 {
		t.Fatal(final)
	}
}
