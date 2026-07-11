package storage

// DefaultListLimit is the page size used when a list request leaves its
// limit unset. The keyset-paginated readers (sessions, skills) fall back
// to it via their own *ListOpts types.
const DefaultListLimit = 50
