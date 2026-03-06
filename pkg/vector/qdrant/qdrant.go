package qdrant

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"

	"github.com/papercomputeco/tapes/pkg/vector"
)

const (
	DefaultCollectionName = "tapes"
	PayloadIDKey          = "id"
	PayloadHashKey        = "hash"
)

// idToUUID converts an arbitrary string ID to a deterministic UUID.
// Qdrant only allows UUIDs and +ve integers as point IDs.
// We store the original ID in the payload.
//
// Ref: https://qdrant.tech/documentation/concepts/points/#point-ids
func idToUUID(id string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(id)).String()
}

type Driver struct {
	client         *qdrant.Client
	collectionName string
	logger         *slog.Logger
	dimensions     uint64
}

type Config struct {
	// Hostname of the Qdrant server. Defaults to "localhost".
	Host string
	// gRPC port of the Qdrant server. Defaults to 6334.
	Port int
	// API key to use for authentication. Defaults to "".
	APIKey string
	// Whether to use TLS for the connection. Defaults to false.
	UseTLS bool
	// Name of the collection to use.
	CollectionName string
	// Dimensions for the embedding vectors.
	Dimensions uint64
}

func NewDriver(c Config, log *slog.Logger) (*Driver, error) {
	if c.Host == "" {
		c.Host = "localhost"
	}
	if c.Port == 0 {
		c.Port = 6334
	}

	if c.Dimensions == 0 {
		return nil, errors.New("qdrant embedding dimensions cannot be 0, must be configured")
	}

	collectionName := c.CollectionName
	if collectionName == "" {
		collectionName = DefaultCollectionName
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   c.Host,
		Port:                   c.Port,
		APIKey:                 c.APIKey,
		UseTLS:                 c.UseTLS,
		SkipCompatibilityCheck: true,
	})
	if err != nil {
		return nil, fmt.Errorf("did not initialize qdrant client: %w", err)
	}

	d := &Driver{
		client:         client,
		collectionName: collectionName,
		logger:         log,
		dimensions:     c.Dimensions,
	}

	err = d.ensureCollection(context.Background())
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("getting or creating collection %q: %w", collectionName, err)
	}

	log.Info("connected to Qdrant",
		"host", c.Host,
		"port", c.Port,
		"tls", c.UseTLS,
		"collection", collectionName,
	)

	return d, nil
}

func (d *Driver) ensureCollection(ctx context.Context) error {
	exists, err := d.client.CollectionExists(ctx, d.collectionName)
	if err != nil {
		return fmt.Errorf("checking if collection exists: %w", err)
	}
	if exists {
		return nil
	}

	err = d.client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: d.collectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     d.dimensions,
			Distance: qdrant.Distance_Cosine,
		}),
	})
	if err != nil {
		return fmt.Errorf("creating collection: %w", err)
	}

	return nil
}

func mapToDocument(payload map[string]*qdrant.Value, embed []float32) vector.Document {
	return vector.Document{
		ID:        payload[PayloadIDKey].GetStringValue(),
		Hash:      payload[PayloadHashKey].GetStringValue(),
		Embedding: embed,
	}
}

func (d *Driver) Add(ctx context.Context, docs []vector.Document) error {
	if len(docs) == 0 {
		return nil
	}

	points := make([]*qdrant.PointStruct, len(docs))
	for i, doc := range docs {
		points[i] = &qdrant.PointStruct{
			Id:      qdrant.NewID(idToUUID(doc.ID)),
			Vectors: qdrant.NewVectorsDense(doc.Embedding),
			Payload: qdrant.NewValueMap(map[string]any{
				PayloadIDKey:   doc.ID,
				PayloadHashKey: doc.Hash,
			}),
		}
	}

	_, err := d.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: d.collectionName,
		Wait:           qdrant.PtrOf(true),
		Points:         points,
	})
	if err != nil {
		return fmt.Errorf("upserting points: %w", err)
	}

	d.logger.Debug("added documents to qdrant", "count", len(docs))
	return nil
}

func (d *Driver) Query(ctx context.Context, embedding []float32, topK int) ([]vector.QueryResult, error) {
	resp, err := d.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: d.collectionName,
		Query:          qdrant.NewQueryDense(embedding),
		Limit:          qdrant.PtrOf(uint64(topK)), //nolint:gosec
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	if err != nil {
		return nil, fmt.Errorf("searching points: %w", err)
	}

	results := make([]vector.QueryResult, len(resp))
	for i, res := range resp {
		embeddings := res.GetVectors().GetVector()
		results[i] = vector.QueryResult{
			Document: mapToDocument(res.Payload, embeddings.GetDenseVector().GetData()),
			Score:    res.Score,
		}
	}

	d.logger.Debug("queried qdrant", "results", len(results))
	return results, nil
}

func (d *Driver) Get(ctx context.Context, ids []string) ([]vector.Document, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	qdrantIDs := make([]*qdrant.PointId, len(ids))
	for i, id := range ids {
		qdrantIDs[i] = qdrant.NewIDUUID(idToUUID(id))
	}

	resp, err := d.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: d.collectionName,
		Ids:            qdrantIDs,
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	if err != nil {
		return nil, fmt.Errorf("getting points: %w", err)
	}

	docs := make([]vector.Document, len(resp))
	for i, pt := range resp {
		embeddings := pt.GetVectors().GetVector()
		docs[i] = mapToDocument(pt.Payload, embeddings.GetDenseVector().GetData())
	}

	return docs, nil
}

func (d *Driver) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	qdrantIDs := make([]*qdrant.PointId, len(ids))
	for i, id := range ids {
		qdrantIDs[i] = qdrant.NewID(idToUUID(id))
	}

	wait := true
	_, err := d.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: d.collectionName,
		Wait:           &wait,
		Points:         qdrant.NewPointsSelectorIDs(qdrantIDs),
	})
	if err != nil {
		return fmt.Errorf("deleting points: %w", err)
	}

	d.logger.Debug("deleted documents from qdrant", "count", len(ids))
	return nil
}

func (d *Driver) Close() error {
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}
