package postgres

import (
	"math"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// requestParamColumns explodes the optional request-envelope parameters
// of a node into their nullable column representations. A nil params
// block (legacy writers, transcript sources that don't know the wire
// envelope) yields all-NULL columns, which readers map back to nil.
func requestParamColumns(p *llm.RequestParams) (
	system pgtype.Text,
	maxTokens pgtype.Int4,
	temperature pgtype.Float8,
	stream pgtype.Bool,
	toolCount pgtype.Int4,
) {
	if p == nil {
		return
	}
	system = nullStringValue(p.System)
	if p.MaxTokens != nil && *p.MaxTokens >= math.MinInt32 && *p.MaxTokens <= math.MaxInt32 {
		maxTokens = pgtype.Int4{Int32: int32(*p.MaxTokens), Valid: true}
	}
	if p.Temperature != nil {
		temperature = pgtype.Float8{Float64: *p.Temperature, Valid: true}
	}
	if p.Stream != nil {
		stream = pgtype.Bool{Bool: *p.Stream, Valid: true}
	}
	if p.ToolCount != nil && *p.ToolCount >= 0 && *p.ToolCount <= math.MaxInt32 {
		toolCount = pgtype.Int4{Int32: int32(*p.ToolCount), Valid: true}
	}
	return
}

// requestParamsFromColumns is the inverse of requestParamColumns.
// Returns nil when every column is NULL so nodes written before the
// raw-capture layer read back exactly as they did.
func requestParamsFromColumns(
	system pgtype.Text,
	maxTokens pgtype.Int4,
	temperature pgtype.Float8,
	stream pgtype.Bool,
	toolCount pgtype.Int4,
) *llm.RequestParams {
	if !system.Valid && !maxTokens.Valid && !temperature.Valid && !stream.Valid && !toolCount.Valid {
		return nil
	}
	p := &llm.RequestParams{System: system.String}
	if maxTokens.Valid {
		v := int(maxTokens.Int32)
		p.MaxTokens = &v
	}
	if temperature.Valid {
		v := temperature.Float64
		p.Temperature = &v
	}
	if stream.Valid {
		v := stream.Bool
		p.Stream = &v
	}
	if toolCount.Valid {
		v := int(toolCount.Int32)
		p.ToolCount = &v
	}
	return p
}
