package derive

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// rederiveChain reconstructs the node chain for one raw turn: parse the
// verbatim request with the row's provider, decode the reduced
// response, and run the shared TurnChain construction. rawOnly is true
// for rows whose response would not have passed ingest validation —
// those rows never produced nodes and are skipped, not failed.
func rederiveChain(providers map[string]provider.Provider, rec *storage.RawTurnRecord, project string) (chain []*merkle.Node, rawOnly bool, err error) {
	prov, ok := providers[rec.Provider]
	if !ok {
		return nil, false, fmt.Errorf("unsupported provider %q", rec.Provider)
	}
	req, err := prov.ParseRequest(rec.RawRequest)
	if err != nil {
		return nil, false, fmt.Errorf("parse request: %w", err)
	}
	var resp llm.ChatResponse
	if len(rec.Response) > 0 {
		if err := json.Unmarshal(rec.Response, &resp); err != nil {
			return nil, false, fmt.Errorf("decode response: %w", err)
		}
	}
	// Mirror ingest's validateReducedResponse gate: a response without
	// a role or content blocks was rejected at ingest (422 after the
	// raw row landed), so no nodes exist to verify against.
	if resp.Message.Role == "" || len(resp.Message.Content) == 0 {
		return nil, true, nil
	}
	chain = TurnChain(CallContext{
		Provider:  rec.Provider,
		AgentName: rec.AgentName,
		ThreadID:  threadIDFromMeta(rec.Meta),
		Project:   project,
	}, req, &resp)
	if len(chain) == 0 {
		return nil, false, errors.New("empty chain")
	}
	return chain, false, nil
}
