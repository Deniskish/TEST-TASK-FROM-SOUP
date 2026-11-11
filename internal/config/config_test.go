package config

import "testing"

func TestRejectUnsafeConfiguration(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://local/db")
	for _, name := range []string{"DB_MAX_CONNS", "LEASE_DURATION", "HEARTBEAT_INTERVAL", "POLL_INTERVAL", "TEST_CLOCK_OFFSET"} {
		t.Setenv(name, "")
	}
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, value string }{{"DB_MAX_CONNS", "0"}, {"DB_MAX_CONNS", "65"}, {"HEARTBEAT_INTERVAL", "2s"}, {"LEASE_DURATION", "-1s"}, {"POLL_INTERVAL", "0s"}} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
