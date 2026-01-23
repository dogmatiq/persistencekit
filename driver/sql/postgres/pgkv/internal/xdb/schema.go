package xdb

import (
	_ "embed"
)

// Schema is the PostgreSQL schema elements required by the kv store.
//
//go:embed schema.sql
var Schema string
