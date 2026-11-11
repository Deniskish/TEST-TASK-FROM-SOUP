package api

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRejectBeforeDatabase(t *testing.T) {
	h := (&API{Token: "secret"}).Handler()
	for _, tc := range []struct {
		body, key, auth, ct string
		code                int
	}{
		{`{"model":"m"}`, "k", "", "application/json", 401},
		{`{"model":"m"}`, "", "Bearer secret", "application/json", 400},
		{`{"model":"m"}`, "k", "Bearer secret", "text/plain", 415},
		{`{"model":"m","unknown":1}`, "k", "Bearer secret", "application/json", 400},
		{`{"model":"m"} {}`, "k", "Bearer secret", "application/json", 400},
		{`null`, "k", "Bearer secret", "application/json", 400},
		{`{"model":"` + strings.Repeat("x", 5000) + `"}`, "k", "Bearer secret", "application/json", 413},
	} {
		r := httptest.NewRequest("POST", "/jobs", strings.NewReader(tc.body))
		r.Header.Set("Idempotency-Key", tc.key)
		r.Header.Set("Authorization", tc.auth)
		r.Header.Set("Content-Type", tc.ct)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.code {
			t.Fatalf("want %d got %d: %s", tc.code, w.Code, w.Body.String())
		}
	}
}
