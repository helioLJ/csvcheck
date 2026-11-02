// Package postgres persists job transitions and their events in one transaction.
package postgres

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/helioLJ/csvcheck/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Store struct {
	Pool  *pgxpool.Pool
	Lease time.Duration
}

func New(ctx context.Context, url string, lease time.Duration) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 12
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return &Store{Pool: p, Lease: lease}, nil
}
func (s *Store) Migrate(ctx context.Context) error { _, err := s.Pool.Exec(ctx, schema); return err }
func hash(b []byte) string                         { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func decode(b []byte) (domain.Job, error) {
	var j domain.Job
	err := json.Unmarshal(b, &j)
	return j, err
}
func save(ctx context.Context, tx pgx.Tx, j *domain.Job, kind string) error {
	j.Sequence++
	j.UpdatedAt = time.Now().UTC()
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE jobs SET state=$2,snapshot=$3 WHERE id=$1", j.ID, j.State, b); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO job_events(job_id,sequence,kind,snapshot) VALUES($1,$2,$3,$4)", j.ID, j.Sequence, kind, b)
	return err
}
func lockJob(ctx context.Context, tx pgx.Tx, id string) (domain.Job, error) {
	var b []byte
	err := tx.QueryRow(ctx, "SELECT snapshot FROM jobs WHERE id=$1 FOR UPDATE", id).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, err
	}
	return decode(b)
}
func (s *Store) AddFile(ctx context.Context, f domain.File) error {
	_, err := s.Pool.Exec(ctx, "INSERT INTO files(id,filename,storage_key,size_bytes,sha256) VALUES($1,$2,$3,$4,$5)", f.ID, f.Filename, f.Key, f.Size, f.SHA256)
	return err
}
func (s *Store) File(ctx context.Context, id string) (domain.File, error) {
	var f domain.File
	err := s.Pool.QueryRow(ctx, "SELECT id,filename,storage_key,size_bytes,sha256 FROM files WHERE id=$1", id).Scan(&f.ID, &f.Filename, &f.Key, &f.Size, &f.SHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		err = domain.ErrNotFound
	}
	return f, err
}
func (s *Store) Submit(ctx context.Context, key, file string, c domain.Config) (domain.Job, error) {
	b, _ := json.Marshal(struct {
		File   string
		Config domain.Config
	}{file, c})
	fingerprint := hash(b)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", key); err != nil {
		return domain.Job{}, err
	}
	var existing []byte
	var oldHash string
	err = tx.QueryRow(ctx, "SELECT snapshot,request_hash FROM jobs WHERE idempotency_key=$1", key).Scan(&existing, &oldHash)
	if err == nil {
		if oldHash != fingerprint {
			return domain.Job{}, domain.ErrConflict
		}
		return decode(existing)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, err
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM files WHERE id=$1)", file).Scan(&exists); err != nil {
		return domain.Job{}, err
	}
	if !exists {
		return domain.Job{}, domain.ErrNotFound
	}
	j := domain.Job{ID: domain.ID(), FileID: file, Config: c, State: "QUEUED", MaxAttempts: 3, CreatedAt: time.Now().UTC()}
	if _, err = tx.Exec(ctx, "INSERT INTO jobs(id,file_id,idempotency_key,request_hash,state,snapshot) VALUES($1,$2,$3,$4,'QUEUED','{}')", j.ID, file, key, fingerprint); err != nil {
		return j, err
	}
	if err = save(ctx, tx, &j, "submitted"); err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}
func (s *Store) Get(ctx context.Context, id string) (domain.Job, error) {
	var b []byte
	err := s.Pool.QueryRow(ctx, "SELECT snapshot FROM jobs WHERE id=$1", id).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, err
	}
	return decode(b)
}
func (s *Store) Events(ctx context.Context, id string, after int64) ([]domain.Event, error) {
	if _, err := s.Get(ctx, id); err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, "SELECT sequence,kind,snapshot FROM job_events WHERE job_id=$1 AND sequence>$2 ORDER BY sequence LIMIT 100", id, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []domain.Event
	for rows.Next() {
		var e domain.Event
		var b []byte
		if err = rows.Scan(&e.Sequence, &e.Kind, &b); err != nil {
			return nil, err
		}
		e.Job, err = decode(b)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
func (s *Store) Claim(ctx context.Context, worker string) (*domain.Assignment, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	var b []byte
	err = tx.QueryRow(ctx, "SELECT snapshot FROM jobs WHERE state='QUEUED' ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1").Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	j, err := decode(b)
	if err != nil {
		return nil, err
	}
	f, err := s.File(ctx, j.FileID)
	if err != nil {
		return nil, err
	}
	a := &domain.Assignment{AttemptRef: domain.AttemptRef{JobID: j.ID, AttemptID: domain.ID(), Token: domain.ID()}, Config: j.Config, InputKey: f.Key, InputSize: f.Size}
	a.OutputPrefix = "attempts/" + j.ID + "/" + a.AttemptID
	j.State = "RUNNING"
	j.AttemptCount++
	j.CurrentAttempt = a.AttemptID
	j.Progress = domain.Progress{}
	j.LastError = ""
	err = tx.QueryRow(ctx, "INSERT INTO job_attempts(id,job_id,number,worker_id,token_hash,expires_at,state) VALUES($1,$2,$3,$4,$5,clock_timestamp()+$6*interval '1 second','RUNNING') RETURNING expires_at", a.AttemptID, j.ID, j.AttemptCount, worker, hash([]byte(a.Token)), s.Lease.Seconds()).Scan(&a.Expires)
	if err != nil {
		return nil, err
	}
	if err = save(ctx, tx, &j, "started"); err != nil {
		return nil, err
	}
	return a, tx.Commit(ctx)
}
func (s *Store) owned(ctx context.Context, tx pgx.Tx, r domain.AttemptRef) (domain.Job, error) {
	j, err := lockJob(ctx, tx, r.JobID)
	if err != nil {
		return j, err
	}
	var valid bool
	err = tx.QueryRow(ctx, "SELECT token_hash=$2 AND expires_at>clock_timestamp() AND state='RUNNING' FROM job_attempts WHERE id=$1 AND job_id=$3", r.AttemptID, hash([]byte(r.Token)), r.JobID).Scan(&valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, domain.ErrStale
	}
	if err != nil {
		return j, err
	}
	if !valid || j.State != "RUNNING" || j.CurrentAttempt != r.AttemptID || j.CancelRequested {
		return j, domain.ErrStale
	}
	return j, nil
}
func (s *Store) Heartbeat(ctx context.Context, r domain.AttemptRef, p domain.Progress) (time.Time, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer rollback(tx)
	j, err := s.owned(ctx, tx, r)
	if err != nil {
		return time.Time{}, err
	}
	f, err := s.File(ctx, j.FileID)
	if err != nil {
		return time.Time{}, err
	}
	if p.BytesConsumed < j.Progress.BytesConsumed || p.RecordsProcessed < j.Progress.RecordsProcessed || p.BytesConsumed > f.Size || p.BytesConsumed < 0 || p.RecordsProcessed < 0 {
		return time.Time{}, domain.ErrInvalid
	}
	j.Progress = p
	var expires time.Time
	err = tx.QueryRow(ctx, "UPDATE job_attempts SET expires_at=clock_timestamp()+$2*interval '1 second' WHERE id=$1 RETURNING expires_at", r.AttemptID, s.Lease.Seconds()).Scan(&expires)
	if err != nil {
		return expires, err
	}
	if err = save(ctx, tx, &j, "progress"); err != nil {
		return expires, err
	}
	return expires, tx.Commit(ctx)
}
func (s *Store) Cancel(ctx context.Context, id string) (domain.Job, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer rollback(tx)
	j, err := lockJob(ctx, tx, id)
	if err != nil {
		return j, err
	}
	if j.Terminal() {
		return j, nil
	}
	j.CancelRequested = true
	j.State = "CANCELLED"
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET state='CANCELLED' WHERE job_id=$1 AND state='RUNNING'", id); err != nil {
		return j, err
	}
	if err = save(ctx, tx, &j, "cancelled"); err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}
func (s *Store) Complete(ctx context.Context, r domain.AttemptRef, sum domain.Summary, artifacts []domain.Artifact) (domain.Job, error) {
	// Include storage keys in the fingerprint even though public JSON hides them.
	records := make([]domain.ArtifactRecord, 0, len(artifacts))
	for _, a := range artifacts {
		records = append(records, domain.ArtifactRecord{Artifact: a, StorageKey: a.Key})
	}
	payload, _ := json.Marshal(struct {
		Summary   domain.Summary
		Artifacts []domain.ArtifactRecord
	}{sum, records})
	fingerprint := hash(payload)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer rollback(tx)
	j, err := lockJob(ctx, tx, r.JobID)
	if err != nil {
		return j, err
	}
	if j.State == "SUCCEEDED" && j.CurrentAttempt == r.AttemptID {
		var old, token string
		err = tx.QueryRow(ctx, "SELECT completion_hash,token_hash FROM job_attempts WHERE id=$1", r.AttemptID).Scan(&old, &token)
		if err != nil {
			return j, err
		}
		if token != hash([]byte(r.Token)) {
			return j, domain.ErrStale
		}
		if old != fingerprint {
			return j, domain.ErrConflict
		}
		return j, nil
	}
	// The job lock is reentrant inside this transaction.
	j, err = s.owned(ctx, tx, r)
	if err != nil {
		return j, err
	}
	for _, a := range artifacts {
		if _, err = tx.Exec(ctx, "INSERT INTO artifacts(job_id,attempt_id,kind,storage_key,size_bytes,sha256) VALUES($1,$2,$3,$4,$5,$6)", j.ID, r.AttemptID, a.Kind, a.Key, a.Size, a.SHA256); err != nil {
			return j, err
		}
	}
	j.State = "SUCCEEDED"
	j.Outcome = "PASSED"
	if sum.Violations > 0 {
		j.Outcome = "FAILED_VALIDATION"
	}
	j.Summary = &sum
	j.Artifacts = artifacts
	j.Progress = domain.Progress{BytesConsumed: sum.BytesConsumed, RecordsProcessed: sum.Records}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET state='SUCCEEDED',completion_hash=$2 WHERE id=$1", r.AttemptID, fingerprint); err != nil {
		return j, err
	}
	if err = save(ctx, tx, &j, "completed"); err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}
func (s *Store) Fail(ctx context.Context, r domain.AttemptRef, code, message string) (domain.Job, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer rollback(tx)
	j, err := s.owned(ctx, tx, r)
	if err != nil {
		return j, err
	}
	j.LastError = code + ": " + message
	j.State = "FAILED"
	if code == "IO_ERROR" && j.AttemptCount < j.MaxAttempts {
		j.State = "QUEUED"
	}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET state='FAILED' WHERE id=$1", r.AttemptID); err != nil {
		return j, err
	}
	if err = save(ctx, tx, &j, "attempt_failed"); err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}
func (s *Store) Reap(ctx context.Context) error {
	// Bounded batches prevent a large recovery backlog from holding many locks.
	for range 100 {
		changed, err := s.reapOne(ctx)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
	}
	return nil
}
func (s *Store) reapOne(ctx context.Context) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	var b []byte
	var attempt string
	err = tx.QueryRow(ctx, "SELECT j.snapshot,a.id FROM jobs j JOIN job_attempts a ON a.job_id=j.id WHERE j.state='RUNNING' AND a.state='RUNNING' AND a.expires_at<=clock_timestamp() ORDER BY a.expires_at FOR UPDATE OF j,a SKIP LOCKED LIMIT 1").Scan(&b, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	j, err := decode(b)
	if err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET state='EXPIRED' WHERE id=$1", attempt); err != nil {
		return false, err
	}
	j.State = "QUEUED"
	if j.AttemptCount >= j.MaxAttempts {
		j.State = "FAILED"
	}
	j.LastError = "LEASE_EXPIRED: worker did not renew its lease"
	if err = save(ctx, tx, &j, "lease_expired"); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
func (s *Store) Artifact(ctx context.Context, id, kind string) (domain.Artifact, error) {
	var a domain.Artifact
	err := s.Pool.QueryRow(ctx, "SELECT kind,storage_key,size_bytes,sha256 FROM artifacts WHERE job_id=$1 AND kind=$2", id, kind).Scan(&a.Kind, &a.Key, &a.Size, &a.SHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		err = domain.ErrNotFound
	}
	return a, err
}
func (s *Store) String() string { return fmt.Sprintf("postgres store (lease=%s)", s.Lease) }
