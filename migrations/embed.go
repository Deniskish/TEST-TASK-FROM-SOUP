package migrations

import _ "embed"

// Schema is applied once by a dedicated Compose migration service.
//
//go:embed 001_init.sql
var Schema string

//go:embed 002_outbox.sql
var Outbox string
