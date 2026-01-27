package storage

// ErrNotFound is returned when a node doesn't exist in the store.
type ErrNotFound struct {
	Hash string
}

func (e ErrNotFound) Error() string {
	if e.Hash == "" {
		return "node not found"
	}

	return "node not found: " + e.Hash
}
