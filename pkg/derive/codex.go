package derive

import (
	"encoding/json"
	"strings"
)

// Codex subagent (thread-spawn) reconciliation — PCC-1021.
//
// Codex Desktop subagents share the ROOT session's harness session key
// on the wire (harness_session_id = the root Codex session id) and carry
// their own thread id per raw turn (meta.thread_id), exactly like Claude
// Code subagents. What Codex does NOT have is Claude's per-agent
// transcript file whose meta.json names the spawning tool_use — the
// spawn join lives in the PARENT rollout's `sub_agent_activity` event:
//
//	{"event_id":"<spawn_agent call_id>", "agent_thread_id":"<child thread id>",
//	 "agent_path":"/root/…", "kind":"started"}
//
// # Spawn-anchor row contract (implemented by the paperd transcript uploader)
//
// paperd ships that evidence as ONE transcript-source raw row per
// spawned child, through the existing transcript ingest endpoint —
// no new tapes ingest surface:
//
//	POST /v1/ingest/transcript  (ingest.TranscriptPayload)
//	{
//	  "session": {
//	    "org_id":             "",
//	    "auth_subject":       "",
//	    "harness_id":         "codex",
//	    "harness_session_id": "<ROOT Codex session id>",  // NEVER the child thread id
//	    "harness_version":    "<parent rollout cli_version>",
//	    "cwd":                "<parent rollout cwd>"
//	  },
//	  "agent_id":    "<child thread id>",       // sub_agent_activity.agent_thread_id
//	  "agent_type":  "<last agent_path segment>",
//	  "description": "<agent_path>",
//	  "tool_use_id": "<spawn_agent call_id>",   // sub_agent_activity.event_id
//	  "records":     [ <the verbatim kind:"started" rollout JSONL line, exactly one> ]
//	}
//
// The endpoint mints the raw row verbatim: source='transcript',
// request_id "transcript:<root sid>:<child thread id>:<sha256(records)[:8]>",
// meta {transcript:true, agent_id, agent_type, description, tool_use_id,
// records:1}. Field semantics:
//
//   - session.harness_session_id MUST be the root session id — the id
//     the parent rollout's session_meta.session_id carries, even for
//     anchors found in a launcher's own rollout (a depth-2 spawn). The
//     reconciler groups files by SessionKey and joins them against
//     chains keyed the same way; a row keyed to the child thread id
//     never meets its chains.
//   - agent_id / tool_use_id are REQUIRED — they are the join. agent_id
//     matches the child raw turns' meta.thread_id; tool_use_id is the
//     spawn_agent function_call call_id, which is also the spawning tool
//     span's span_id in the projection.
//   - agent_type / description become the console panel labels
//     (args.subagent_type / args.description on the spawn tool span's
//     tool_input, see spawnToolInput). A wire-envelope agent_nickname,
//     when present, refines agent_type at derive time.
//   - records is a JSON array holding exactly the one kind:"started"
//     rollout line, verbatim. Only "started" records are anchors —
//     "interacted" (followup_task, send_message) is not a spawn. The
//     content is evidence provenance only: the identity join reads
//     agent_id/tool_use_id from the row meta and never needs record
//     content; ParseTranscriptFile accepts any object array (unknown
//     fields are ignored, no content signatures result).
//   - One row per spawned child, uploaded after the "started" event is
//     observed. The endpoint dedups on content hash, so re-uploads are
//     idempotent no-ops.
//
// # Why the join is per-CALL, not per-chain
//
// Claude's reconciler stamps fork edges on chain ROOT nodes and
// threadAnchor reads them back. Codex thread chains routinely do not
// own their roots: a fork_turns:"all" child re-sends the parent's
// history, so its chain grafts onto the root spine's nodes; two
// fork_turns:"none" siblings start from byte-identical harness context,
// so they share one deduped chain root. In both shapes a node-level
// stamp either misses or collides (the shared root carries only the
// last writer's edge — the same reason permission checks moved to
// per-call anchors). So Codex threads anchor per CALL: every thread
// call's SpanSource.Anchor carries its spawn tool_use id, and
// threadAnchor prefers it over the node stamp.
//
// # Degrade ladder
//
//  1. Identity join (exact): anchor row agent_id == thread id.
//  2. agent_path fallback (exact but ambiguous under reuse): the parent
//     spine's spawn_agent function_call_output is {"task_name":"<agent
//     path>"}, and the child's wire envelope harness_metadata carries the
//     same path (source.subagent.thread_spawn.agent_path). Joined only
//     while a path maps to exactly one spawn call; a reused path is
//     ambiguous and refuses to guess.
//  3. No anchor: the thread's agent span parents to the trace root —
//     visible in the trace, never attached to a wrong tool. Counted in
//     ReconcileStats.CodexThreadsUnanchored.

// harnessCodex is the harness id Codex raw turns carry.
const harnessCodex = "codex"

// toolSpawnAgent is Codex's subagent-spawning collaboration tool.
const toolSpawnAgent = "spawn_agent"

// sub_agent_activity lifecycle kinds, as carried in anchor-row meta
// (and in the verbatim record itself). Only started records are spawn
// evidence. Interacted records — send_message / followup_task
// re-entries, uploaded by paperd for durability, whose target may be
// the sender's parent or the ROOT — MUST stay inert in derivation: an
// interacted row never anchors a thread, never overrides a started
// anchor, and a thread whose only rows are interacted degrades exactly
// as if it had no anchor at all (PCC-1021 decision C).
const (
	subAgentKindStarted    = "started"
	subAgentKindInteracted = "interacted"
)

// CodexThreadMeta is the per-thread spawn metadata a Codex child raw
// turn carries in its session envelope's harness_metadata — the
// agent_path fallback join key plus the console-facing nickname.
type CodexThreadMeta struct {
	AgentPath string
	Nickname  string
}

// SpawnLabel is the console-facing labeling of one spawn tool_use,
// resolved during reconciliation and folded into the spawning tool
// span's tool_input by the span emitter (see spawnToolInput).
type SpawnLabel struct {
	SubagentType string
	Description  string
}

// codexEnvelopeMeta is the slice of a raw turn's session envelope the
// Codex reconciler reads.
type codexEnvelopeMeta struct {
	HarnessMetadata struct {
		// Source is the child's thread-spawn provenance. paperd emits it
		// as a JSON-encoded string; the rollout's session_meta form is an
		// object — both are accepted.
		Source json.RawMessage `json:"source"`
	} `json:"harness_metadata"`
}

// codexSpawnSource mirrors source.subagent.thread_spawn.
type codexSpawnSource struct {
	Subagent struct {
		ThreadSpawn struct {
			AgentPath     string `json:"agent_path"`
			AgentNickname string `json:"agent_nickname"`
		} `json:"thread_spawn"`
	} `json:"subagent"`
}

// codexThreadMetaFromEnvelope extracts the thread-spawn metadata from a
// child raw turn's session envelope, or nil when the envelope carries
// none (root turns, non-subagent sources, older paperd builds).
func codexThreadMetaFromEnvelope(envelope json.RawMessage) *CodexThreadMeta {
	if len(envelope) == 0 {
		return nil
	}
	var env codexEnvelopeMeta
	if json.Unmarshal(envelope, &env) != nil || len(env.HarnessMetadata.Source) == 0 {
		return nil
	}
	raw := env.HarnessMetadata.Source
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		raw = json.RawMessage(asString)
	}
	var src codexSpawnSource
	if json.Unmarshal(raw, &src) != nil {
		return nil
	}
	spawn := src.Subagent.ThreadSpawn
	if spawn.AgentPath == "" {
		return nil
	}
	return &CodexThreadMeta{AgentPath: spawn.AgentPath, Nickname: spawn.AgentNickname}
}

// spawnTaskName decodes a spawn_agent function_call_output payload —
// {"task_name":"/root/…"} — and returns the task name ("" otherwise).
func spawnTaskName(output string) string {
	var out struct {
		TaskName string `json:"task_name"`
	}
	if json.Unmarshal([]byte(output), &out) != nil {
		return ""
	}
	return out.TaskName
}

// lastPathSegment returns the final segment of an agent path
// ("/root/depth2_cli_child" → "depth2_cli_child").
func lastPathSegment(path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	return segs[len(segs)-1]
}

// reconcileCodexSpawns anchors every Codex thread call to the
// spawn_agent tool_use that created its thread, per the degrade ladder
// above. Runs inside ReconcileTranscripts, after the Claude chain join;
// pure and re-runnable — stamping is idempotent.
func reconcileCodexSpawns(set *DerivedSet, files []*TranscriptFile, stats *ReconcileStats) {
	// Identity map from the spawn-anchor rows. Kind-filtered: only
	// started (or legacy unmarked) rows are spawn evidence — an
	// interacted row joined here would re-anchor its TARGET thread to
	// the send/followup call, or clobber the real started anchor.
	fileByThread := map[SessionKey]map[string]*TranscriptFile{}
	for _, f := range files {
		if f.Session.HarnessID != harnessCodex || f.AgentID == "" || f.ToolUseID == "" {
			continue
		}
		if !f.SpawnEvidence() {
			stats.CodexInteractedRows++
			continue
		}
		byThread := fileByThread[f.Session]
		if byThread == nil {
			byThread = map[string]*TranscriptFile{}
			fileByThread[f.Session] = byThread
		}
		byThread[f.AgentID] = f
	}

	// spawn_agent inventory over the capture: tool_use ids, and the
	// task_name each spawn's function_call_output reported — the
	// agent_path fallback join. A task_name that maps to more than one
	// spawn id is ambiguous and dropped.
	type spawnJoin struct {
		byPath    map[string]string // agent_path -> spawn tool_use id
		ambiguous map[string]bool
	}
	joins := map[SessionKey]*spawnJoin{}
	for _, src := range set.SpanSources {
		if src.Session.HarnessID != harnessCodex {
			continue
		}
		j := joins[src.Session]
		if j == nil {
			j = &spawnJoin{byPath: map[string]string{}, ambiguous: map[string]bool{}}
			joins[src.Session] = j
		}
		for i, dn := range src.Chain {
			if !src.New[i] {
				continue
			}
			for _, b := range dn.Node.Bucket.Content {
				if b.Type != blockToolResult || b.ToolResultID == "" {
					continue
				}
				name := spawnTaskName(b.ToolOutput)
				if name == "" {
					continue
				}
				if prev, ok := j.byPath[name]; ok && prev != b.ToolResultID {
					j.ambiguous[name] = true
					continue
				}
				j.byPath[name] = b.ToolResultID
			}
		}
	}

	// Anchor every Codex thread call; resolve panel labels per spawn.
	anchored := map[string]bool{} // session|thread -> had an anchor
	seen := map[string]bool{}
	if set.SpawnLabels == nil {
		set.SpawnLabels = map[string]SpawnLabel{}
	}
	for _, src := range set.SpanSources {
		if src.Session.HarnessID != harnessCodex || src.ThreadID == "" {
			continue
		}
		meta := set.CodexThreads[src.Session][src.ThreadID]

		var anchor string
		var label SpawnLabel
		if f := fileByThread[src.Session][src.ThreadID]; f != nil {
			anchor = f.ToolUseID
			label = SpawnLabel{SubagentType: f.AgentType, Description: f.Description}
		} else if j := joins[src.Session]; j != nil && meta.AgentPath != "" && !j.ambiguous[meta.AgentPath] {
			anchor = j.byPath[meta.AgentPath]
		}
		// The wire envelope's nickname/path refine or fill the labels.
		if meta.Nickname != "" {
			label.SubagentType = meta.Nickname
		}
		if label.Description == "" {
			label.Description = meta.AgentPath
		}
		if label.SubagentType == "" && meta.AgentPath != "" {
			label.SubagentType = lastPathSegment(meta.AgentPath)
		}

		threadKey := src.Session.HarnessID + "|" + src.Session.HarnessSessionID + "|" + src.ThreadID
		if anchor != "" {
			src.Anchor = anchor
			anchored[threadKey] = true
			if label != (SpawnLabel{}) {
				set.SpawnLabels[toolKey(src.Session, anchor)] = label
			}
		}
		seen[threadKey] = true
	}

	for key := range seen {
		if anchored[key] {
			stats.CodexThreadsAnchored++
		} else {
			stats.CodexThreadsUnanchored++
		}
	}
}
