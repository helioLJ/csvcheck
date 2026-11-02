// Package domain defines transport-independent validation and job models.
package domain

import (
	"crypto/rand"
	"errors"
	"time"
)

var (
	ErrInvalid  = errors.New("invalid request")
	ErrNotFound = errors.New("resource not found")
	ErrConflict = errors.New("request conflicts with existing state")
	ErrStale    = errors.New("attempt no longer owns the job")
)

func ID() string { return rand.Text() }

type Config struct {
	Delimiter string `json:"delimiter"`
	Rules     []Rule `json:"rules"`
}
type Rule struct {
	Column         string   `json:"column"`
	RequiredColumn bool     `json:"required_column"`
	RequiredValue  bool     `json:"required_value"`
	Decimal        *Decimal `json:"decimal,omitempty"`
	Date           *Date    `json:"date,omitempty"`
	Text           *Text    `json:"text,omitempty"`
}
type Decimal struct {
	Minimum          *string `json:"minimum,omitempty"`
	Maximum          *string `json:"maximum,omitempty"`
	MinimumExclusive bool    `json:"minimum_exclusive"`
	MaximumExclusive bool    `json:"maximum_exclusive"`
}
type Date struct {
	Format string `json:"format"`
}
type Text struct {
	MinLength *uint32 `json:"min_length,omitempty"`
	MaxLength *uint32 `json:"max_length,omitempty"`
}
type File struct {
	ID       string `json:"id"`
	Filename string `json:"filename"`
	Key      string `json:"-"`
	Size     int64  `json:"size_bytes"`
	SHA256   string `json:"sha256"`
}
type Summary struct {
	Records           int64 `json:"records"`
	InvalidRecords    int64 `json:"invalid_records"`
	Violations        int64 `json:"violations"`
	SampledViolations int64 `json:"sampled_violations"`
	BytesConsumed     int64 `json:"bytes_consumed"`
}
type Progress struct {
	BytesConsumed    int64 `json:"bytes_consumed"`
	RecordsProcessed int64 `json:"records_processed"`
}
type Artifact struct {
	Kind   string `json:"kind"`
	Key    string `json:"-"`
	Size   int64  `json:"size_bytes"`
	SHA256 string `json:"sha256"`
}

// ArtifactRecord persists the storage key without exposing it in public snapshots.
type ArtifactRecord struct {
	Artifact
	StorageKey string `json:"storage_key"`
}
type Job struct {
	ID              string     `json:"id"`
	FileID          string     `json:"file_id"`
	Config          Config     `json:"config"`
	State           string     `json:"state"`
	Outcome         string     `json:"validation_outcome,omitempty"`
	AttemptCount    int        `json:"attempt_count"`
	MaxAttempts     int        `json:"max_attempts"`
	CurrentAttempt  string     `json:"current_attempt,omitempty"`
	CancelRequested bool       `json:"cancel_requested"`
	Progress        Progress   `json:"progress"`
	Summary         *Summary   `json:"summary,omitempty"`
	Artifacts       []Artifact `json:"artifacts,omitempty"`
	LastError       string     `json:"last_error,omitempty"`
	Sequence        int64      `json:"last_event_sequence"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (j Job) Terminal() bool {
	return j.State == "SUCCEEDED" || j.State == "FAILED" || j.State == "CANCELLED"
}

type Event struct {
	Sequence int64
	Kind     string
	Job      Job
}
type AttemptRef struct{ JobID, AttemptID, Token string }
type Assignment struct {
	AttemptRef
	Expires      time.Time
	InputKey     string
	InputSize    int64
	Config       Config
	OutputPrefix string
}
