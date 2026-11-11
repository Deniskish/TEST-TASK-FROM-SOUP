package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL, Addr, Token            string
	PoolSize                            int32
	Lease, Heartbeat, Poll, ClockOffset time.Duration
}

func Load() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("DATABASE_URL"), Addr: os.Getenv("HTTP_ADDR"), Token: os.Getenv("API_TOKEN")}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL required")
	}
	if c.Addr == "" {
		c.Addr = ":8080"
	}
	n := 8
	if v := os.Getenv("DB_MAX_CONNS"); v != "" {
		var err error
		n, err = strconv.Atoi(v)
		if err != nil {
			return c, errors.New("invalid DB_MAX_CONNS")
		}
	}
	if n < 1 || n > 64 {
		return c, errors.New("DB_MAX_CONNS must be 1..64")
	}
	c.PoolSize = int32(n)
	values := []struct {
		name, def string
		dst       *time.Duration
	}{{"LEASE_DURATION", "3s", &c.Lease}, {"HEARTBEAT_INTERVAL", "750ms", &c.Heartbeat}, {"POLL_INTERVAL", "100ms", &c.Poll}, {"TEST_CLOCK_OFFSET", "0s", &c.ClockOffset}}
	for _, v := range values {
		s := os.Getenv(v.name)
		if s == "" {
			s = v.def
		}
		d, e := time.ParseDuration(s)
		if e != nil {
			return c, errors.New("invalid " + v.name)
		}
		*v.dst = d
	}
	if c.Lease < 300*time.Millisecond || c.Lease > time.Minute || c.Heartbeat <= 0 || c.Heartbeat > c.Lease/3 || c.Poll <= 0 {
		return c, errors.New("require lease 300ms..1m, 0 < heartbeat <= lease/3, poll > 0")
	}
	return c, nil
}
