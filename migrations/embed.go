package migrationfiles

import "embed"

// Files contains the immutable SQL migrations shipped with this source tree.
//
//go:embed *.sql
var Files embed.FS
