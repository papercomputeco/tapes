package vector

import "errors"

var (
	// ErrNotFound is returned when a document is not found in the vector store.
	ErrNotFound = errors.New("document not found")

	// ErrEmbedding is returned when embedding generation fails.
	ErrEmbedding = errors.New("embedding failed")

	// ErrConnection is returned when the vector store connection fails.
	ErrConnection = errors.New("vector store connection failed")
)
