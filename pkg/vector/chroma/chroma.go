// Package chroma provides a Chroma vector database driver implementation.
package chroma

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/vector"
)

const (
	// DefaultCollectionName is the default collection name for storing tapes embeddings.
	DefaultCollectionName = "tapes"

	// defaultMaxConnectRetries is the maximum number of retries when connecting to Chroma.
	defaultMaxConnectRetries = 10

	// defaultRetryDelay is the initial delay between retries. Each subsequent
	// retry doubles the delay (exponential backoff), capped at maxRetryDelay.
	defaultRetryDelay = 1 * time.Second

	// defaultMaxRetryDelay is the maximum delay between retries.
	defaultMaxRetryDelay = 15 * time.Second
)

// Driver implements vector.Driver using Chroma's REST API.
type Driver struct {
	baseURL        string
	collectionName string
	collectionID   string
	httpClient     *http.Client
	logger         *zap.Logger
}

// Config holds configuration for the Chroma driver.
type Config struct {
	// URL is the Chroma server URL (e.g., "http://localhost:8000").
	URL string

	// CollectionName is the name of the collection to use.
	// Defaults to DefaultCollectionName if empty.
	CollectionName string

	// MaxRetries overrides the maximum number of connection retries.
	// Defaults to maxConnectRetries if zero.
	MaxRetries uint

	// RetryDelay overrides the initial delay between retries.
	// Defaults to initialRetryDelay if zero.
	RetryDelay time.Duration

	// MaxRetryDelay overrides the maximum delay between retries.
	// Defaults to maxRetryDelay if zero.
	MaxRetryDelay time.Duration
}

// NewDriver creates a new Chroma vector driver.
// It uses exponential delay retries if it cannot connect to Chroma.
func NewDriver(c Config, logger *zap.Logger) (*Driver, error) {
	if c.URL == "" {
		return nil, errors.New("chroma URL is required")
	}

	collectionName := c.CollectionName
	if collectionName == "" {
		collectionName = DefaultCollectionName
	}

	d := &Driver{
		baseURL:        c.URL,
		collectionName: collectionName,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		logger: logger,
	}

	if c.MaxRetries == 0 {
		c.MaxRetries = defaultMaxConnectRetries
	}

	if c.RetryDelay <= 0 {
		c.RetryDelay = defaultRetryDelay
	}

	if c.MaxRetryDelay <= 0 {
		c.MaxRetryDelay = defaultMaxRetryDelay
	}

	var err error
	collectionID := ""
	for attempt := range c.MaxRetries {
		collectionID, err = d.getOrCreateCollection(context.Background())
		if err == nil {
			break
		}

		if attempt == c.MaxRetries-1 {
			return nil, fmt.Errorf("getting or creating collection %q after %d attempts: %w", collectionName, c.MaxRetries, err)
		}

		logger.Warn("failed to connect to Chroma, retrying...",
			zap.Uint("attempt", attempt+1),
			zap.Duration("delay", c.RetryDelay),
			zap.Error(err),
		)

		time.Sleep(c.RetryDelay)
		c.RetryDelay *= 2
		if c.RetryDelay > c.MaxRetryDelay {
			c.RetryDelay = c.MaxRetryDelay
		}
	}
	d.collectionID = collectionID

	logger.Info("connected to Chroma",
		zap.String("url", c.URL),
		zap.String("collection", collectionName),
		zap.String("collection_id", collectionID),
	)

	return d, nil
}

// getOrCreateCollection gets an existing collection or creates a new one.
func (d *Driver) getOrCreateCollection(ctx context.Context) (string, error) {
	// Try to get existing collection first
	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s", d.baseURL, d.collectionName)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("creating get request: %w", err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("sending get request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var collection chromaCollection
		if err := json.NewDecoder(resp.Body).Decode(&collection); err != nil {
			return "", fmt.Errorf("decoding collection response: %w", err)
		}
		return collection.ID, nil
	}

	// Collection doesn't exist, create it
	createURL := d.baseURL + "/api/v2/tenants/default_tenant/databases/default_database/collections"
	createBody := map[string]string{"name": d.collectionName}
	jsonBody, err := json.Marshal(createBody)
	if err != nil {
		return "", fmt.Errorf("marshaling create request: %w", err)
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodPost, createURL, bytes.NewReader(jsonBody))
	if err != nil {
		return "", fmt.Errorf("creating create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err = d.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("sending create request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to create collection: status %d: %s", resp.StatusCode, string(body))
	}

	var collection chromaCollection
	if err := json.NewDecoder(resp.Body).Decode(&collection); err != nil {
		return "", fmt.Errorf("decoding create response: %w", err)
	}

	return collection.ID, nil
}

// Add stores documents with their embeddings.
func (d *Driver) Add(ctx context.Context, docs []vector.Document) error {
	if len(docs) == 0 {
		return nil
	}

	ids := make([]string, len(docs))
	embeddings := make([][]float32, len(docs))
	metadatas := make([]map[string]any, len(docs))

	for i, doc := range docs {
		ids[i] = doc.ID
		embeddings[i] = doc.Embedding
		metadatas[i] = map[string]any{"hash": doc.Hash}
	}

	reqBody := chromaAddRequest{
		IDs:        ids,
		Embeddings: embeddings,
		Metadatas:  metadatas,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshaling add request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/add", d.baseURL, d.collectionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("creating add request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending add request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to add documents: status %d: %s", resp.StatusCode, string(body))
	}

	d.logger.Debug("added documents to chroma",
		zap.Int("count", len(docs)),
	)

	return nil
}

// Query finds the topK most similar documents to the given embedding.
func (d *Driver) Query(ctx context.Context, embedding []float32, topK int) ([]vector.QueryResult, error) {
	if topK <= 0 {
		topK = 10
	}

	reqBody := chromaQueryRequest{
		QueryEmbeddings: [][]float32{embedding},
		NResults:        topK,
		Include:         []string{"metadatas", "distances", "embeddings"},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling query request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/query", d.baseURL, d.collectionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("creating query request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending query request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to query: status %d: %s", resp.StatusCode, string(body))
	}

	var queryResp chromaQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("decoding query response: %w", err)
	}

	var results []vector.QueryResult

	// Process first group (we only query with one embedding)
	if len(queryResp.IDs) == 0 || len(queryResp.IDs[0]) == 0 {
		return results, nil
	}

	ids := queryResp.IDs[0]
	distances := queryResp.Distances[0]

	var metadatas []map[string]any
	if len(queryResp.Metadatas) > 0 {
		metadatas = queryResp.Metadatas[0]
	}

	var embeddings [][]float32
	if len(queryResp.Embeddings) > 0 {
		embeddings = queryResp.Embeddings[0]
	}

	for i, id := range ids {
		result := vector.QueryResult{
			Document: vector.Document{
				ID:   id,
				Hash: id, // Default to ID
			},
		}

		// Extract hash from metadata
		if i < len(metadatas) && metadatas[i] != nil {
			if hash, ok := metadatas[i]["hash"].(string); ok {
				result.Hash = hash
			}
		}

		// Add embedding if available
		if i < len(embeddings) {
			result.Embedding = embeddings[i]
		}

		// Convert distance to similarity score
		// Lower distance = higher similarity
		if i < len(distances) {
			result.Score = 1.0 / (1.0 + distances[i])
		}

		results = append(results, result)
	}

	d.logger.Debug("queried chroma",
		zap.Int("results", len(results)),
	)

	return results, nil
}

// Get retrieves documents by their IDs.
func (d *Driver) Get(ctx context.Context, ids []string) ([]vector.Document, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	reqBody := chromaGetRequest{
		IDs:     ids,
		Include: []string{"metadatas", "embeddings"},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling get request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/get", d.baseURL, d.collectionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("creating get request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending get request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get documents: status %d: %s", resp.StatusCode, string(body))
	}

	var getResp chromaGetResponse
	if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
		return nil, fmt.Errorf("decoding get response: %w", err)
	}

	docs := make([]vector.Document, len(getResp.IDs))
	for i, id := range getResp.IDs {
		docs[i] = vector.Document{
			ID:   id,
			Hash: id, // Default to ID
		}

		// Extract hash from metadata
		if i < len(getResp.Metadatas) && getResp.Metadatas[i] != nil {
			if hash, ok := getResp.Metadatas[i]["hash"].(string); ok {
				docs[i].Hash = hash
			}
		}

		// Add embedding if available
		if i < len(getResp.Embeddings) {
			docs[i].Embedding = getResp.Embeddings[i]
		}
	}

	return docs, nil
}

// Delete removes documents by their IDs.
func (d *Driver) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	reqBody := chromaDeleteRequest{
		IDs: ids,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshaling delete request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/delete", d.baseURL, d.collectionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("creating delete request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending delete request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete documents: status %d: %s", resp.StatusCode, string(body))
	}

	d.logger.Debug("deleted documents from chroma",
		zap.Int("count", len(ids)),
	)

	return nil
}

// Close releases resources held by the driver.
func (d *Driver) Close() error {
	// HTTP client doesn't require explicit cleanup
	return nil
}
