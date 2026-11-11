package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"soup-job-queue/internal/jobs"
	"soup-job-queue/internal/storage"
)

type API struct {
	Store *storage.Store
	Token string
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", a.create)
	mux.HandleFunc("GET /jobs/{id}", a.get)
	mux.HandleFunc("GET /jobs", a.list)
	mux.HandleFunc("POST /jobs/{id}/cancel", a.cancel)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := a.Store.Pool.Ping(r.Context()); err != nil {
			reply(w, 503, map[string]string{"error": "database unavailable"})
			return
		}
		reply(w, 200, map[string]string{"status": "ok"})
	})
	slots := make(chan struct{}, 64)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path != "/health" && a.Token != "" {
			expected := sha256.Sum256([]byte("Bearer " + a.Token))
			actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
			if subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
				reply(w, 401, map[string]string{"error": "unauthorized"})
				return
			}
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			w.Header().Set("Retry-After", "1")
			reply(w, 503, map[string]string{"error": "busy"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		reply(w, 404, map[string]string{"error": "job not found"})
	case errors.Is(err, jobs.ErrConflict), errors.Is(err, jobs.ErrTerminal):
		reply(w, 409, map[string]string{"error": err.Error()})
	default:
		reply(w, 503, map[string]string{"error": "database operation unavailable; retry with same key"})
	}
}
func id(r *http.Request) (int64, error) {
	n, e := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if e != nil || n <= 0 {
		return 0, errors.New("id must be a positive integer")
	}
	return n, nil
}
func (a *API) create(w http.ResponseWriter, r *http.Request) {
	if !jobs.ValidKey(r.Header.Get("Idempotency-Key")) {
		reply(w, 400, map[string]string{"error": "Idempotency-Key must be 1..128 printable ASCII bytes without spaces"})
		return
	}
	if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
		reply(w, 415, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req jobs.Request
	err := dec.Decode(&req)
	if err == nil {
		var extra any
		if e := dec.Decode(&extra); e != io.EOF {
			err = errors.New("expected exactly one JSON object")
			var large *http.MaxBytesError
			if errors.As(e, &large) {
				err = e
			}
		}
	}
	if err == nil {
		err = req.Normalize()
	}
	if err != nil {
		var large *http.MaxBytesError
		status := 400
		if errors.As(err, &large) {
			status = 413
		}
		reply(w, status, map[string]string{"error": "invalid request: " + err.Error()})
		return
	}
	j, created, err := a.Store.Create(r.Context(), r.Header.Get("Idempotency-Key"), req)
	if err != nil {
		fail(w, err)
		return
	}
	status := 200
	if created {
		status = 201
	}
	w.Header().Set("Location", "/jobs/"+strconv.FormatInt(j.ID, 10))
	reply(w, status, j)
}
func (a *API) get(w http.ResponseWriter, r *http.Request) {
	n, e := id(r)
	if e != nil {
		reply(w, 400, map[string]string{"error": e.Error()})
		return
	}
	j, e := a.Store.Get(r.Context(), n)
	if e != nil {
		fail(w, e)
		return
	}
	reply(w, 200, j)
}
func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	n, e := id(r)
	if e != nil {
		reply(w, 400, map[string]string{"error": e.Error()})
		return
	}
	j, e := a.Store.Cancel(r.Context(), n)
	if e != nil {
		fail(w, e)
		return
	}
	reply(w, 200, j)
}
func (a *API) list(w http.ResponseWriter, r *http.Request) {
	after := int64(0)
	limit := 50
	var err error
	if v := r.URL.Query().Get("after"); v != "" {
		after, err = strconv.ParseInt(v, 10, 64)
	}
	if err != nil || after < 0 {
		reply(w, 400, map[string]string{"error": "invalid after cursor"})
		return
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
	}
	if err != nil || limit < 1 || limit > 100 {
		reply(w, 400, map[string]string{"error": "limit must be 1..100"})
		return
	}
	list, err := a.Store.List(r.Context(), after, limit)
	if err != nil {
		fail(w, err)
		return
	}
	next := after
	if len(list) > 0 {
		next = list[len(list)-1].ID
	}
	reply(w, 200, map[string]any{"jobs": list, "next_after": next})
}
