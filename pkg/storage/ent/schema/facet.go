package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Facet holds the schema definition for the Facet entity.
// This stores LLM-extracted qualitative facets for sessions.
type Facet struct {
	ent.Schema
}

// Fields of the Facet.
func (Facet) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").
			Unique().
			Immutable().
			NotEmpty(),

		field.String("session_id").
			NotEmpty(),

		field.JSON("facets", map[string]any{}).
			Optional(),

		field.Time("created_at").
			Default(time.Now).
			Immutable().
			Annotations(entsql.Default("CURRENT_TIMESTAMP")),
	}
}

// Indexes of the Facet.
func (Facet) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("session_id").
			Unique(),
	}
}
