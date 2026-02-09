package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// AgentTrace holds the schema definition for the AgentTrace entity.
// This is the root record for an agent trace.
type AgentTrace struct {
	ent.Schema
}

// Fields of the AgentTrace.
func (AgentTrace) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").
			Unique().
			Immutable().
			NotEmpty(),

		field.String("version").
			NotEmpty(),

		field.String("timestamp").
			NotEmpty(),

		field.String("vcs_type").
			Optional(),

		field.String("vcs_revision").
			Optional(),

		field.String("tool_name").
			Optional(),

		field.String("tool_version").
			Optional(),

		field.JSON("metadata", map[string]any{}).
			Optional(),

		field.Time("created_at").
			Default(time.Now).
			Immutable().
			Annotations(entsql.Default("CURRENT_TIMESTAMP")),
	}
}

// Indexes of the AgentTrace.
func (AgentTrace) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("vcs_revision"),
		index.Fields("tool_name"),
		index.Fields("timestamp"),
	}
}

// Edges of the AgentTrace.
func (AgentTrace) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("files", AgentTraceFile.Type),
	}
}
