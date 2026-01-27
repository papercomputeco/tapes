package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Node holds the schema definition for the Node entity.
// This represents a content-addressed node in the Merkle DAG
// storing LLM conversation turns.
type Node struct {
	ent.Schema
}

// Fields of the Node.
func (Node) Fields() []ent.Field {
	return []ent.Field{
		// id is the content-addressed identifier (SHA-256, hex-encoded)
		// This serves as the primary key
		field.String("id").
			StorageKey("hash").
			Unique().
			Immutable().
			NotEmpty(),

		// parent_hash links to the previous node hash
		// This will be nil/null for root nodes
		field.String("parent_hash").
			Optional().
			Nillable(),

		// bucket is the raw bucket JSON for full data preservation
		field.JSON("bucket", map[string]any{}).
			Optional(),

		// type identifies the kind of content (e.g., "message")
		field.String("type").
			Optional(),

		// role indicates who produced this message ("system", "user", "assistant", "tool")
		field.String("role").
			Optional(),

		// content holds the message content blocks as JSON
		field.JSON("content", []map[string]any{}).
			Optional(),

		// model identifies the LLM model (e.g., "gpt-4", "claude-3-sonnet")
		field.String("model").
			Optional(),

		// provider identifies the API provider (e.g., "openai", "anthropic", "ollama")
		field.String("provider").
			Optional(),

		// stop_reason indicates why generation stopped (only for responses)
		field.String("stop_reason").
			Optional(),

		// prompt_tokens is the number of prompt tokens used
		field.Int("prompt_tokens").
			Optional().
			Nillable(),

		// completion_tokens is the number of completion tokens generated
		field.Int("completion_tokens").
			Optional().
			Nillable(),

		// total_tokens is the total number of tokens (prompt + completion)
		field.Int("total_tokens").
			Optional().
			Nillable(),

		// total_duration_ns is the total duration in nanoseconds
		field.Int64("total_duration_ns").
			Optional().
			Nillable(),

		// prompt_duration_ns is the prompt processing duration in nanoseconds
		field.Int64("prompt_duration_ns").
			Optional().
			Nillable(),

		// created_at is the timestamp when the node was created
		field.Time("created_at").
			Default(time.Now).
			Immutable().
			Annotations(entsql.Default("CURRENT_TIMESTAMP")),
	}
}

// Indexes of the Node.
func (Node) Indexes() []ent.Index {
	return []ent.Index{
		// Index on parent_hash for efficient child lookups
		index.Fields("parent_hash"),

		// Index on role for filtering by message role
		index.Fields("role"),

		// Index on model for filtering by model
		index.Fields("model"),

		// Index on provider for filtering by provider
		index.Fields("provider"),

		// Composite index for common query patterns
		index.Fields("role", "model"),
	}
}

// Edges of the Node.
func (Node) Edges() []ent.Edge {
	return []ent.Edge{
		// Parent edge: each node has at most one parent
		edge.To("parent", Node.Type).
			Field("parent_hash").
			Unique(),

		// Children edge: each node can have multiple children
		edge.From("children", Node.Type).
			Ref("parent"),
	}
}
