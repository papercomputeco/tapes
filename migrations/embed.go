package migrations

import "embed"

// FS contains the embedded SQL migration files.
//
//go:embed *.sql
var FS embed.FS
