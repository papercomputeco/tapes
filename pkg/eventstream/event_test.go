package eventstream_test

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/eventstream"
	"github.com/papercomputeco/tapes/pkg/llm"
)

var _ = Describe("Event", func() {
	It("marshals TurnPersistedEvent with expected top-level keys", func() {
		now := time.Unix(1735689600, 0).UTC()
		parentHash := "parent-hash"
		event := eventstream.TurnPersistedEvent{
			SchemaVersion: eventstream.SchemaVersionV1,
			EventType:     eventstream.EventTypeTurnPersisted,
			EventID:       "evt_123",
			EmittedAt:     now,
			Source: eventstream.EventSource{
				Project:   "my-project",
				AgentName: "codex",
				Provider:  "openai",
			},
			RequestMeta: eventstream.TurnRequestMeta{
				Path:        "/v1/chat/completions",
				StartedAt:   now.Add(-2 * time.Second),
				CompletedAt: now,
				DurationMs:  2000,
				Streaming:   true,
				HTTPStatus:  200,
			},
			DAG: eventstream.TurnDAGMeta{
				RootHash:       "root-hash",
				HeadHash:       "head-hash",
				ParentHash:     &parentHash,
				TurnNodeHashes: []string{"n1", "n2"},
				NewNodeHashes:  []string{"n2"},
			},
			Turn: llm.ConversationTurn{
				Provider: "openai",
				Request: &llm.ChatRequest{
					Model: "gpt-4.1",
					Messages: []llm.Message{
						llm.NewTextMessage("user", "hello"),
					},
				},
				Response: &llm.ChatResponse{
					Model:   "gpt-4.1",
					Message: llm.NewTextMessage("assistant", "hi"),
					Done:    true,
				},
			},
		}

		payload, err := json.Marshal(event)
		Expect(err).NotTo(HaveOccurred())

		var got map[string]any
		Expect(json.Unmarshal(payload, &got)).To(Succeed())

		Expect(got).To(HaveKey("schema_version"))
		Expect(got).To(HaveKey("event_type"))
		Expect(got).To(HaveKey("event_id"))
		Expect(got).To(HaveKey("emitted_at"))
		Expect(got).To(HaveKey("source"))
		Expect(got).To(HaveKey("request_meta"))
		Expect(got).To(HaveKey("dag"))
		Expect(got).To(HaveKey("turn"))
	})

	It("defines stable event constants", func() {
		Expect(eventstream.SchemaVersionV1).To(BeNumerically(">", 0))
		Expect(eventstream.EventTypeTurnPersisted).To(Equal("tapes.turn.persisted"))
	})

	It("provides ErrNilTurnEvent for nil payload validation", func() {
		Expect(eventstream.ErrNilTurnEvent).NotTo(BeNil())
		Expect(eventstream.ErrNilTurnEvent).To(MatchError("nil turn event"))
	})
})
