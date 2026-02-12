package memory

import "errors"

// ErrNotConfigured is returned when memory operations are attempted
// but no memory driver has been configured.
var ErrNotConfigured = errors.New("memory not configured")
