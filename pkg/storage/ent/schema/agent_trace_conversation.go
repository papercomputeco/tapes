package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// AgentTraceConversation holds the schema definition for the AgentTraceConversation entity.
type AgentTraceConversation struct {
	ent.Schema
}

// Fields of the AgentTraceConversation.
func (AgentTraceConversation) Fields() []ent.Field {
	return []ent.Field{
		field.String("url").
			Optional(),

		field.String("contributor_type").
			Optional(),

		field.String("contributor_model_id").
			Optional(),

		field.JSON("related", []map[string]any{}).
			Optional(),
	}
}

// Indexes of the AgentTraceConversation.
func (AgentTraceConversation) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("contributor_type"),
		index.Fields("contributor_model_id"),
	}
}

// Edges of the AgentTraceConversation.
func (AgentTraceConversation) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("file", AgentTraceFile.Type).
			Ref("conversations").
			Unique().
			Required(),

		edge.To("ranges", AgentTraceRange.Type),
	}
}
