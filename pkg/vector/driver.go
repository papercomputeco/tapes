// Package vector provides interfaces and implementations for vector storage and embedding.
package vector

import "context"

// Document represents a stored item with its embedding and metadata.
type Document struct {
	// ID is a unique identifier for the document (typically the node hash).
	ID string

	// Hash is the merkle node hash this document corresponds to.
	Hash string

	// Embedding is the vector representation of the document content.
	Embedding []float32
}

// QueryResult represents a search result with similarity score.
type QueryResult struct {
	Document

	// Score represents the similarity score (higher = more similar).
	Score float32
}

// VectorDriver handles storage and retrieval of vector embeddings.
type VectorDriver interface {
	// Add stores documents with their embeddings.
	// If a document with the same ID already exists, implementers should update
	// the document.
	Add(ctx context.Context, docs []Document) error

	// Query finds the topK most similar documents to the given embedding.
	Query(ctx context.Context, embedding []float32, topK int) ([]QueryResult, error)

	// Get retrieves documents by their IDs.
	Get(ctx context.Context, ids []string) ([]Document, error)

	// Delete removes documents by their IDs.
	Delete(ctx context.Context, ids []string) error

	// Close releases any resources held by the driver.
	Close() error
}
