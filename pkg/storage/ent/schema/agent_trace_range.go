package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// AgentTraceRange holds the schema definition for the AgentTraceRange entity.
type AgentTraceRange struct {
	ent.Schema
}

// Fields of the AgentTraceRange.
func (AgentTraceRange) Fields() []ent.Field {
	return []ent.Field{
		field.Int("start_line"),

		field.Int("end_line"),

		field.String("content_hash").
			Optional(),

		field.String("contributor_type").
			Optional(),

		field.String("contributor_model_id").
			Optional(),
	}
}

// Indexes of the AgentTraceRange.
func (AgentTraceRange) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("start_line", "end_line"),
	}
}

// Edges of the AgentTraceRange.
func (AgentTraceRange) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("conversation", AgentTraceConversation.Type).
			Ref("ranges").
			Unique().
			Required(),
	}
}
