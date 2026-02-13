package entdriver

import (
	"context"
	"fmt"

	"github.com/papercomputeco/tapes/pkg/agenttrace"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
	entagenttrace "github.com/papercomputeco/tapes/pkg/storage/ent/agenttrace"
	"github.com/papercomputeco/tapes/pkg/storage/ent/agenttracefile"
	"github.com/papercomputeco/tapes/pkg/storage/ent/predicate"
)

// EntAgentTraceDriver provides agent trace storage operations using an ent client.
type EntAgentTraceDriver struct {
	Client *ent.Client
}

// CreateAgentTrace stores an agent trace using a transaction for atomic multi-table insert.
func (d *EntAgentTraceDriver) CreateAgentTrace(ctx context.Context, trace *agenttrace.AgentTrace) (*agenttrace.AgentTrace, error) {
	tx, err := d.Client.Tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("starting transaction: %w", err)
	}

	// Create the root trace record
	traceCreate := tx.AgentTrace.Create().
		SetID(trace.ID).
		SetVersion(trace.Version).
		SetTimestamp(trace.Timestamp)

	if trace.VCS != nil {
		if trace.VCS.Type != "" {
			traceCreate.SetVcsType(trace.VCS.Type)
		}
		if trace.VCS.Revision != "" {
			traceCreate.SetVcsRevision(trace.VCS.Revision)
		}
	}

	if trace.Tool != nil {
		if trace.Tool.Name != "" {
			traceCreate.SetToolName(trace.Tool.Name)
		}
		if trace.Tool.Version != "" {
			traceCreate.SetToolVersion(trace.Tool.Version)
		}
	}

	if trace.Metadata != nil {
		traceCreate.SetMetadata(trace.Metadata)
	}

	entTrace, err := traceCreate.Save(ctx)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("creating agent trace: %w", err)
	}

	// Create files and their nested entities
	for _, f := range trace.Files {
		fileCreate := tx.AgentTraceFile.Create().
			SetPath(f.Path).
			SetTrace(entTrace)

		entFile, err := fileCreate.Save(ctx)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("creating agent trace file: %w", err)
		}

		for _, conv := range f.Conversations {
			convCreate := tx.AgentTraceConversation.Create().
				SetFile(entFile)

			if conv.URL != "" {
				convCreate.SetURL(conv.URL)
			}
			if conv.Contributor != nil {
				if conv.Contributor.Type != "" {
					convCreate.SetContributorType(conv.Contributor.Type)
				}
				if conv.Contributor.ModelID != "" {
					convCreate.SetContributorModelID(conv.Contributor.ModelID)
				}
			}
			if len(conv.RelatedResources) > 0 {
				related := make([]map[string]any, len(conv.RelatedResources))
				for i, r := range conv.RelatedResources {
					related[i] = map[string]any{
						"type": r.Type,
						"url":  r.URL,
					}
				}
				convCreate.SetRelated(related)
			}

			entConv, err := convCreate.Save(ctx)
			if err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("creating agent trace conversation: %w", err)
			}

			for _, r := range conv.Ranges {
				rangeCreate := tx.AgentTraceRange.Create().
					SetStartLine(r.StartLine).
					SetEndLine(r.EndLine).
					SetConversation(entConv)

				if r.ContentHash != "" {
					rangeCreate.SetContentHash(r.ContentHash)
				}
				if r.Contributor != nil {
					if r.Contributor.Type != "" {
						rangeCreate.SetContributorType(r.Contributor.Type)
					}
					if r.Contributor.ModelID != "" {
						rangeCreate.SetContributorModelID(r.Contributor.ModelID)
					}
				}

				if _, err := rangeCreate.Save(ctx); err != nil {
					_ = tx.Rollback()
					return nil, fmt.Errorf("creating agent trace range: %w", err)
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing transaction: %w", err)
	}

	return d.GetAgentTrace(ctx, trace.ID)
}

// GetAgentTrace retrieves an agent trace by ID with eager loading 3 levels deep.
func (d *EntAgentTraceDriver) GetAgentTrace(ctx context.Context, id string) (*agenttrace.AgentTrace, error) {
	entTrace, err := d.Client.AgentTrace.Query().
		Where(entagenttrace.ID(id)).
		WithFiles(func(q *ent.AgentTraceFileQuery) {
			q.WithConversations(func(q *ent.AgentTraceConversationQuery) {
				q.WithRanges()
			})
		}).
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, fmt.Errorf("agent trace not found: %s", id)
		}
		return nil, fmt.Errorf("querying agent trace: %w", err)
	}

	return entAgentTraceToAgentTrace(entTrace), nil
}

// QueryAgentTraces queries agent traces with dynamic filtering.
func (d *EntAgentTraceDriver) QueryAgentTraces(ctx context.Context, query storage.AgentTraceQuery) ([]*agenttrace.AgentTrace, error) {
	q := d.Client.AgentTrace.Query()

	// Apply filters
	var predicates []predicate.AgentTrace

	if query.FilePath != "" {
		predicates = append(predicates, entagenttrace.HasFilesWith(agenttracefile.PathEQ(query.FilePath)))
	}
	if query.Revision != "" {
		predicates = append(predicates, entagenttrace.VcsRevisionEQ(query.Revision))
	}
	if query.ToolName != "" {
		predicates = append(predicates, entagenttrace.ToolNameEQ(query.ToolName))
	}

	if len(predicates) > 0 {
		q.Where(predicates...)
	}

	// Apply ordering
	q.Order(ent.Desc(entagenttrace.FieldCreatedAt))

	// Apply pagination
	if query.Limit > 0 {
		q.Limit(query.Limit)
	}
	if query.Offset > 0 {
		q.Offset(query.Offset)
	}

	// Eager load all nested entities
	q.WithFiles(func(fq *ent.AgentTraceFileQuery) {
		fq.WithConversations(func(cq *ent.AgentTraceConversationQuery) {
			cq.WithRanges()
		})
	})

	entTraces, err := q.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying agent traces: %w", err)
	}

	traces := make([]*agenttrace.AgentTrace, 0, len(entTraces))
	for _, et := range entTraces {
		traces = append(traces, entAgentTraceToAgentTrace(et))
	}

	return traces, nil
}

// Close closes the ent client.
func (d *EntAgentTraceDriver) Close() error {
	return d.Client.Close()
}

// entAgentTraceToAgentTrace converts an ent AgentTrace entity to a domain AgentTrace.
func entAgentTraceToAgentTrace(et *ent.AgentTrace) *agenttrace.AgentTrace {
	trace := &agenttrace.AgentTrace{
		Version:   et.Version,
		ID:        et.ID,
		Timestamp: et.Timestamp,
		Metadata:  et.Metadata,
	}

	if et.VcsType != "" || et.VcsRevision != "" {
		trace.VCS = &agenttrace.VCS{
			Type:     et.VcsType,
			Revision: et.VcsRevision,
		}
	}

	if et.ToolName != "" || et.ToolVersion != "" {
		trace.Tool = &agenttrace.Tool{
			Name:    et.ToolName,
			Version: et.ToolVersion,
		}
	}

	if et.Edges.Files != nil {
		trace.Files = make([]agenttrace.File, len(et.Edges.Files))
		for i, ef := range et.Edges.Files {
			trace.Files[i] = entAgentTraceFileToFile(ef)
		}
	}

	return trace
}

func entAgentTraceFileToFile(ef *ent.AgentTraceFile) agenttrace.File {
	file := agenttrace.File{
		Path: ef.Path,
	}

	if ef.Edges.Conversations != nil {
		file.Conversations = make([]agenttrace.Conversation, len(ef.Edges.Conversations))
		for i, ec := range ef.Edges.Conversations {
			file.Conversations[i] = entAgentTraceConversationToConversation(ec)
		}
	}

	return file
}

func entAgentTraceConversationToConversation(ec *ent.AgentTraceConversation) agenttrace.Conversation {
	conv := agenttrace.Conversation{
		URL: ec.URL,
	}

	if ec.ContributorType != "" || ec.ContributorModelID != "" {
		conv.Contributor = &agenttrace.Contributor{
			Type:    ec.ContributorType,
			ModelID: ec.ContributorModelID,
		}
	}

	if ec.Related != nil {
		conv.RelatedResources = make([]agenttrace.RelatedResource, len(ec.Related))
		for i, r := range ec.Related {
			rr := agenttrace.RelatedResource{}
			if t, ok := r["type"].(string); ok {
				rr.Type = t
			}
			if u, ok := r["url"].(string); ok {
				rr.URL = u
			}
			conv.RelatedResources[i] = rr
		}
	}

	if ec.Edges.Ranges != nil {
		conv.Ranges = make([]agenttrace.Range, len(ec.Edges.Ranges))
		for i, er := range ec.Edges.Ranges {
			conv.Ranges[i] = entAgentTraceRangeToRange(er)
		}
	}

	return conv
}

func entAgentTraceRangeToRange(er *ent.AgentTraceRange) agenttrace.Range {
	r := agenttrace.Range{
		StartLine:   er.StartLine,
		EndLine:     er.EndLine,
		ContentHash: er.ContentHash,
	}

	if er.ContributorType != "" || er.ContributorModelID != "" {
		r.Contributor = &agenttrace.Contributor{
			Type:    er.ContributorType,
			ModelID: er.ContributorModelID,
		}
	}

	return r
}
