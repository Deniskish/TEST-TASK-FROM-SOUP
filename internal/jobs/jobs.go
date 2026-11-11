package jobs

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrConflict  = errors.New("idempotency key already used with a different payload")
	ErrTerminal  = errors.New("job already completed")
	ErrLostLease = errors.New("lease expired or ownership lost")
)

type Request struct {
	Model            string `json:"model"`
	DurationMS       int    `json:"duration_ms"`
	MaxAttempts      int    `json:"max_attempts"`
	FailUntilAttempt int    `json:"fail_until_attempt"`
}

func (r *Request) Normalize() error {
	r.Model = strings.TrimSpace(r.Model)
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 5
	}
	if len(r.Model) == 0 || len(r.Model) > 128 {
		return errors.New("model must contain 1..128 bytes")
	}
	if r.DurationMS < 0 || r.DurationMS > 60000 {
		return errors.New("duration_ms must be 0..60000")
	}
	if r.MaxAttempts < 1 || r.MaxAttempts > 20 {
		return errors.New("max_attempts must be 1..20")
	}
	if r.FailUntilAttempt < 0 || r.FailUntilAttempt > 20 {
		return errors.New("fail_until_attempt must be 0..20")
	}
	return nil
}

func ValidKey(key string) bool {
	if len(key) < 1 || len(key) > 128 {
		return false
	}
	for _, c := range key {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

type Job struct {
	ID          int64      `json:"id"`
	Payload     Request    `json:"payload"`
	Status      string     `json:"status"`
	Attempts    int        `json:"attempts"`
	MaxAttempts int        `json:"max_attempts"`
	Token       int64      `json:"-"`
	LeaseUntil  *time.Time `json:"lease_until,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	LastError   string     `json:"last_error,omitempty"`
}
