package derive

// Reconciliation fuses the two sources of truth: the wire capture is
// the complete call inventory, the harness transcript is the
// authoritative causal/fork skeleton. The join key is projected
// content — the same projection the chain hash uses — so a wire chain
// whose blocks overwhelmingly appear in subagent agent-X's transcript
// IS agent-X's thread, and its root gains the fork edge
// (parent_tool_use_id = the Task tool_use that spawned the agent, from
// the harness's subagent meta.json). The rejoin needs no new edge: the
// Task tool_result in the main chain already carries the same id.

// ReconcileStats reports the transcript↔wire fusion for one org.
type ReconcileStats struct {
	TranscriptFiles  int `json:"transcript_files"`
	SubagentForks    int `json:"subagent_forks"`
	ForkedChains     int `json:"forked_chains"`
	MainChainsJoined int `json:"main_chains_joined"`

	// ConversationJoined / ConversationTotal measure how many
	// conversation-spine nodes' content appears in a transcript — the
	// Go-native version of the prototype's join-rate oracle.
	ConversationJoined int `json:"conversation_joined"`
	ConversationTotal  int `json:"conversation_total"`
}

// ReconcileTranscripts assigns each wire-derived conversation chain to
// the transcript file whose content it matches, stamping subagent
// chain roots with their fork edge. Operates on the in-memory derived
// set after the wire pass and before the store write — pure and
// re-runnable like everything else in the deriver.
func ReconcileTranscripts(set *DerivedSet, files []*TranscriptFile) *ReconcileStats {
	stats := &ReconcileStats{TranscriptFiles: len(files)}
	if len(files) == 0 || len(set.Nodes) == 0 {
		return stats
	}

	// Group transcript files per session.
	bySession := map[SessionKey][]*TranscriptFile{}
	for _, f := range files {
		bySession[f.Session] = append(bySession[f.Session], f)
	}

	// Children index over the derived nodes (spine links only).
	children := map[string][]*DerivedNode{}
	var roots []*DerivedNode
	for _, dn := range set.Nodes {
		if dn.Node.Kind != KindMain {
			continue
		}
		if dn.Node.ParentHash == nil {
			roots = append(roots, dn)
			continue
		}
		children[*dn.Node.ParentHash] = append(children[*dn.Node.ParentHash], dn)
	}

	for _, root := range roots {
		candidates := bySession[root.Session]
		if len(candidates) == 0 {
			continue
		}

		// Walk this root's chain and score block overlap per file.
		overlap := make(map[*TranscriptFile]int, len(candidates))
		var joined, total int
		stack := []*DerivedNode{root}
		seen := map[string]struct{}{}
		for len(stack) > 0 {
			dn := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if _, dup := seen[dn.Node.Hash]; dup {
				continue
			}
			seen[dn.Node.Hash] = struct{}{}
			stack = append(stack, children[dn.Node.Hash]...)

			nodeMatched := false
			for _, block := range dn.Node.Bucket.Content {
				sig := blockSignature(block)
				if sig == "" {
					continue
				}
				for _, f := range candidates {
					if _, ok := f.signatures[sig]; ok {
						overlap[f]++
						nodeMatched = true
					}
				}
			}
			total++
			if nodeMatched {
				joined++
			}
		}
		stats.ConversationTotal += total
		stats.ConversationJoined += joined

		var best *TranscriptFile
		for _, f := range candidates {
			if best == nil || overlap[f] > overlap[best] {
				best = f
			}
		}
		if best == nil || overlap[best] == 0 {
			continue
		}
		if best.AgentID == "" {
			stats.MainChainsJoined++
			continue
		}
		if best.ToolUseID == "" {
			continue
		}
		// Subagent thread: fork edge at the chain root.
		root.Node.ParentToolUseID = best.ToolUseID
		stats.ForkedChains++
	}

	subagents := map[string]struct{}{}
	for _, f := range files {
		if f.AgentID != "" && f.ToolUseID != "" {
			subagents[f.ToolUseID] = struct{}{}
		}
	}
	stats.SubagentForks = len(subagents)
	return stats
}
