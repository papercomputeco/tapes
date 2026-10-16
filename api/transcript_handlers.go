package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/transcript"
)

// GET /v1/sessions/{id}/transcript is the session as one readable
// conversation: the user's words and the assistant's replies in order,
// classified on the server so every client reads the same thing. The
// rendering itself lives in pkg/transcript; this file is the page around
// it — the filters, the cursor, and the bounds.
//
// A page is materialized, not streamed: it is bounded in rows (`limit`)
// and in bytes, and spans are read one at a time from the store, so the
// resident set is one page of rows plus the span being folded, whatever
// the session's size. Filtering happens while walking, so a narrow filter
// may read far past the rows it returns; the cursor marks where the walk
// stopped, not how many rows were kept.

const (
	defaultTranscriptLimit = 200
	maxTranscriptLimit     = 1000
)

// transcriptPageByteBudget bounds one page's encoded rows. Rows carry
// whole tool results, so a row count alone does not bound a page; once
// the rows pass this many bytes the page closes and the rest continues
// behind next_cursor. A variable so a spec can shrink it.
var transcriptPageByteBudget = 8 << 20

// TranscriptResponse is one page of a session's transcript.
type TranscriptResponse struct {
	SessionID string           `json:"session_id"`
	Items     []transcript.Row `json:"items"`
	// NextCursor continues the walk from the last row of this page (pass
	// it as `cursor`). Absent once no further row matches the filters. A
	// page may close short of `limit` on its byte budget, so its absence —
	// not the page's length — is what means "no more".
	NextCursor string `json:"next_cursor,omitempty"`
}

// transcriptQuery is a parsed transcript request: the same parameters
// the HTTP query string and the MCP tool both carry.
type transcriptQuery struct {
	SessionID string
	Role      string
	Q         string
	Subagents bool
	Limit     int
	Cursor    string
}

// transcriptError is a failure with the HTTP status it maps to, so the
// HTTP handler and the MCP tool share one builder and report the same way.
type transcriptError struct {
	status int
	msg    string
}

func (e *transcriptError) Error() string { return e.msg }

func transcriptFail(status int, msg string) error {
	return &transcriptError{status: status, msg: msg}
}

// transcriptPageCursor is the decoded transcript cursor: the trace the
// last served row belongs to (in the (started_at, trace_id) order traces
// are walked), the key of the last span folded within it (empty span_id:
// from the trace's first span), and, when the page ended between a call's
// user row and its assistant row, the span whose user row was served.
type transcriptPageCursor struct {
	Session        string    `json:"session"`
	TraceID        string    `json:"trace_id"`
	TraceStartedAt time.Time `json:"trace_started_at"`
	Seq            int64     `json:"seq,omitempty"`
	StartedAt      time.Time `json:"started_at,omitzero"`
	SpanID         string    `json:"span_id,omitempty"`
	SkipUserOf     string    `json:"skip_user_of,omitempty"`
}

func decodeTranscriptPageCursor(token string) (transcriptPageCursor, error) {
	c, err := decodeCursor[transcriptPageCursor](token)
	if err != nil {
		return transcriptPageCursor{}, err
	}
	if c.Session == "" || c.TraceID == "" {
		return transcriptPageCursor{}, errors.New("invalid cursor: missing trace boundary")
	}
	return c, nil
}

// before reports whether turn sorts strictly before the cursor's trace —
// a trace an earlier page finished.
func (c transcriptPageCursor) before(turn storage.SpanTurnRecord) bool {
	if turn.StartedAt.Before(c.TraceStartedAt) {
		return true
	}
	return turn.StartedAt.Equal(c.TraceStartedAt) && turn.TraceID < c.TraceID
}

func (c transcriptPageCursor) position() transcript.Position {
	pos := transcript.Position{SkipUserOf: c.SkipUserOf}
	if c.SpanID != "" {
		pos.After = storage.SpanCursor{TraceID: c.TraceID, Seq: c.Seq, StartedAt: c.StartedAt, SpanID: c.SpanID}
	}
	return pos
}

func newTranscriptPageCursor(session string, turn storage.SpanTurnRecord, pos transcript.Position) transcriptPageCursor {
	return transcriptPageCursor{
		Session:        session,
		TraceID:        turn.TraceID,
		TraceStartedAt: turn.StartedAt,
		Seq:            pos.After.Seq,
		StartedAt:      pos.After.StartedAt,
		SpanID:         pos.After.SpanID,
		SkipUserOf:     pos.SkipUserOf,
	}
}

// parseTranscriptRole accepts user|assistant|all, defaulting to all.
func parseTranscriptRole(raw string) (string, error) {
	switch raw {
	case "", "all":
		return "all", nil
	case transcript.RoleUser, transcript.RoleAssistant:
		return raw, nil
	default:
		return "", errors.New("role must be one of user, assistant, all")
	}
}

// handleGetSessionTranscript handles GET /v1/sessions/:id/transcript.
func (s *Server) handleGetSessionTranscript(c fiber.Ctx) error {
	q := transcriptQuery{
		SessionID: c.Params("id"),
		Role:      c.Query("role"),
		Q:         c.Query("q"),
		Cursor:    c.Query("cursor"),
	}
	if raw := c.Query("subagents"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "subagents must be true or false"})
		}
		q.Subagents = v
	}
	limit, err := parseLimit(c.Query("limit"), defaultTranscriptLimit, maxTranscriptLimit)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	q.Limit = limit

	resp, err := s.buildTranscript(c.Context(), q)
	if err != nil {
		var te *transcriptError
		if errors.As(err, &te) {
			return c.Status(te.status).JSON(llm.ErrorResponse{Error: te.msg})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load transcript"})
	}
	return c.JSON(resp)
}

// buildTranscript reads one page of a session's transcript. Every failure
// is a *transcriptError carrying its HTTP status.
func (s *Server) buildTranscript(ctx context.Context, q transcriptQuery) (*TranscriptResponse, error) {
	sessions, ok := s.driver.(sessionsReader)
	if !ok {
		return nil, transcriptFail(fiber.StatusNotImplemented, "sessions not supported by this backend")
	}
	reader, ok := s.driver.(spanModelReader)
	if !ok {
		return nil, transcriptFail(fiber.StatusNotImplemented, "span traces not supported by this backend")
	}

	if q.SessionID == "" {
		return nil, transcriptFail(fiber.StatusBadRequest, "id parameter required")
	}
	if _, err := uuid.Parse(q.SessionID); err != nil {
		return nil, transcriptFail(fiber.StatusBadRequest, "id must be a valid UUID")
	}
	role, err := parseTranscriptRole(strings.TrimSpace(q.Role))
	if err != nil {
		return nil, transcriptFail(fiber.StatusBadRequest, err.Error())
	}
	limit := q.Limit
	if limit <= 0 {
		limit = defaultTranscriptLimit
	}
	limit = min(limit, maxTranscriptLimit)

	var cursor *transcriptPageCursor
	if q.Cursor != "" {
		cur, err := decodeTranscriptPageCursor(q.Cursor)
		if err != nil {
			return nil, transcriptFail(fiber.StatusBadRequest, err.Error())
		}
		if cur.Session != q.SessionID {
			return nil, transcriptFail(fiber.StatusBadRequest, "cursor does not match session")
		}
		cursor = &cur
	}

	orgID := singleTenantOrgID
	sess, err := sessions.GetSessionRecord(ctx, orgID, q.SessionID)
	if err != nil {
		s.logger.Error("get session for transcript", "id", q.SessionID, "error", err)
		return nil, transcriptFail(fiber.StatusInternalServerError, "failed to load session")
	}
	if sess == nil {
		return nil, transcriptFail(fiber.StatusNotFound, "session not found")
	}

	turns, err := reader.ListTraceSummaries(ctx, q.SessionID)
	if err != nil {
		s.logger.Error("list trace summaries for transcript", "session_id", q.SessionID, "error", err)
		return nil, transcriptFail(fiber.StatusInternalServerError, "failed to load transcript")
	}

	page := &transcriptPage{
		sessionID: q.SessionID,
		role:      role,
		q:         q.Q,
		opts:      transcript.Options{Subagents: q.Subagents},
		limit:     limit,
		budget:    transcriptPageByteBudget,
		resp:      &TranscriptResponse{SessionID: q.SessionID, Items: []transcript.Row{}},
	}
	if err := page.walk(ctx, reader, orgID, turns, cursor); err != nil {
		s.logger.Error("walk transcript", "session_id", q.SessionID, "error", err)
		return nil, transcriptFail(fiber.StatusInternalServerError, "failed to load transcript")
	}
	return page.resp, nil
}

// transcriptPage is one page being filled.
type transcriptPage struct {
	sessionID string
	role      string
	q         string
	opts      transcript.Options
	limit     int
	budget    int
	bytes     int
	resp      *TranscriptResponse
	// next is the cursor minted at the last row this page kept; it is
	// published only once a further matching row proves there is more.
	next *transcriptPageCursor
	full bool
	more bool
}

// walk folds the session's traces in turn order, from the cursor on,
// until the page is full and one more matching row has been seen.
func (p *transcriptPage) walk(ctx context.Context, reader spanModelReader, orgID string, turns []storage.TraceSummaryRecord, cursor *transcriptPageCursor) error {
	for _, turn := range turns {
		var pos transcript.Position
		if cursor != nil {
			if cursor.before(turn.SpanTurnRecord) {
				continue
			}
			if turn.TraceID == cursor.TraceID {
				pos = cursor.position()
			}
		}
		walker := transcript.NewWalker(p.opts, pos)
		for sp, err := range reader.IterateTraceSpans(ctx, orgID, turn.TraceID, pos.After, storage.PayloadFull) {
			if err != nil {
				return fmt.Errorf("iterate trace %s: %w", turn.TraceID, err)
			}
			if p.accept(turn.SpanTurnRecord, walker.Feed(sp)) {
				break
			}
		}
		if p.more {
			break
		}
		p.accept(turn.SpanTurnRecord, walker.Flush())
		if p.more {
			break
		}
	}
	if p.more && p.next != nil {
		p.resp.NextCursor = encodeCursor(*p.next)
	}
	return nil
}

// accept keeps the emitted rows that match the filters and reports
// whether the walk can stop: the page is full and another matching row
// exists past it.
func (p *transcriptPage) accept(turn storage.SpanTurnRecord, rows []transcript.Emitted) bool {
	for _, e := range rows {
		if !transcript.Matches(e.Row, p.role, p.q) {
			continue
		}
		if p.full {
			p.more = true
			return true
		}
		encoded, err := json.Marshal(e.Row)
		if err == nil {
			p.bytes += len(encoded)
		}
		p.resp.Items = append(p.resp.Items, e.Row)
		cur := newTranscriptPageCursor(p.sessionID, turn, e.Next)
		p.next = &cur
		if len(p.resp.Items) >= p.limit || p.bytes > p.budget {
			p.full = true
		}
	}
	return false
}
