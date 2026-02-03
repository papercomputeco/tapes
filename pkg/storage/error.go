package storage

// NotFoundError is returned when a node doesn't exist in the store.
type NotFoundError struct {
	Hash string
}

func (e NotFoundError) Error() string {
	if e.Hash == "" {
		return "node not found"
	}

	return "node not found: " + e.Hash
}
