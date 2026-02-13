package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// AgentTraceFile holds the schema definition for the AgentTraceFile entity.
type AgentTraceFile struct {
	ent.Schema
}

// Fields of the AgentTraceFile.
func (AgentTraceFile) Fields() []ent.Field {
	return []ent.Field{
		field.String("path").
			NotEmpty(),
	}
}

// Indexes of the AgentTraceFile.
func (AgentTraceFile) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("path"),
	}
}

// Edges of the AgentTraceFile.
func (AgentTraceFile) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("trace", AgentTrace.Type).
			Ref("files").
			Unique().
			Required(),

		edge.To("conversations", AgentTraceConversation.Type),
	}
}
