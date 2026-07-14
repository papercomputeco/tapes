package derive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// verifyPageSize is the raw-turn scan batch. Verification is an
// operator action (admin endpoint), not a hot path; the value just
// bounds memory.
const verifyPageSize = 200

// maxReportedMissing caps the per-report sample lists so a wholly
// missing store doesn't produce a megabyte of hashes.
const maxReportedMissing = 20

// VerifyResult reports the raw→derived round-trip: re-deriving node
// chains from the immutable raw layer and checking every derived hash
// against the node store. This is the Phase-1 definition-of-done for
// the raw-capture layer — when MissingNodes is empty, the derived
// layer is a pure function of raw and can be rebuilt at will.
type VerifyResult struct {
	// RawTurns is the number of raw rows scanned.
	RawTurns int `json:"raw_turns"`

	// ParsedTurns is how many raw rows parsed cleanly into a
	// request/response pair.
	ParsedTurns int `json:"parsed_turns"`

	// ParseFailures lists raw rows whose request or response no longer
	// parses (capped). Non-empty means a provider parser regressed OR
	// a raw row predates a parser fix — either way the raw row is
	// intact and re-derivable later.
	ParseFailures []string `json:"parse_failures,omitempty"`

	// RawOnlyTurns counts raw rows whose response fails the same
	// validation ingest applies (missing role/content — error
	// captures, aborted streams). Ingest never derived nodes for
	// these, so neither does the verifier; the raw row is their only
	// representation, by design.
	RawOnlyTurns int `json:"raw_only_turns"`

	// DerivedNodes is the number of distinct node hashes produced by
	// re-deriving every parsed raw turn.
	DerivedNodes int `json:"derived_nodes"`

	// PresentNodes is how many derived hashes exist in the node store.
	PresentNodes int `json:"present_nodes"`

	// MissingNodes samples derived hashes absent from the node store
	// (capped at 20). Empty means the round-trip reproduces the
	// captured nodes exactly.
	MissingNodes []string `json:"missing_nodes,omitempty"`
}

// Verified reports whether the round-trip was lossless.
func (r *VerifyResult) Verified() bool {
	return len(r.MissingNodes) == 0 && r.DerivedNodes == r.PresentNodes
}

// VerifyRederive scans the entire raw layer, re-derives each turn's
// node chain with the same construction the ingest worker uses, and
// checks every derived hash against the node store.
//
// project mirrors the ingest worker's configured project tag; it does
// not participate in node hashes, so any value verifies identically.
func VerifyRederive(ctx context.Context, raw storage.RawTurnStore, nodes storage.Driver, project string) (*VerifyResult, error) {
	providers := make(map[string]provider.Provider)
	for _, name := range provider.SupportedProviders() {
		prov, err := provider.New(name)
		if err != nil {
			return nil, fmt.Errorf("create provider %s: %w", name, err)
		}
		providers[name] = prov
	}

	result := &VerifyResult{}
	derived := make(map[string]struct{})

	var afterID int64
	for {
		page, err := raw.ListRawTurns(ctx, afterID, verifyPageSize)
		if err != nil {
			return nil, fmt.Errorf("list raw turns after %d: %w", afterID, err)
		}
		if len(page) == 0 {
			break
		}
		for i := range page {
			rec := &page[i]
			afterID = rec.ID
			result.RawTurns++

			chain, rawOnly, err := rederiveChain(providers, rec, project)
			if rawOnly {
				result.RawOnlyTurns++
				continue
			}
			if err != nil {
				if len(result.ParseFailures) < maxReportedMissing {
					result.ParseFailures = append(result.ParseFailures,
						fmt.Sprintf("raw_turn id=%d request_id=%s: %v", rec.ID, rec.RequestID, err))
				}
				continue
			}
			result.ParsedTurns++
			for _, node := range chain {
				derived[node.Hash] = struct{}{}
			}
		}
	}

	result.DerivedNodes = len(derived)
	for hash := range derived {
		ok, err := nodes.Has(ctx, hash)
		if err != nil {
			return nil, fmt.Errorf("check node %s: %w", hash, err)
		}
		if ok {
			result.PresentNodes++
			continue
		}
		if len(result.MissingNodes) < maxReportedMissing {
			result.MissingNodes = append(result.MissingNodes, hash)
		}
	}

	return result, nil
}

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
	threadID := threadIDFromMeta(rec.Meta)
	if threadID == "" && req.Extra != nil {
		threadID, _ = req.Extra["thread_id"].(string)
	}
	chain = TurnChain(CallContext{
		Provider:  rec.Provider,
		AgentName: rec.AgentName,
		ThreadID:  threadID,
		Project:   project,
	}, req, &resp)
	if len(chain) == 0 {
		return nil, false, errors.New("empty chain")
	}
	return chain, false, nil
}
