package deck

import (
	"context"
	"errors"
	"fmt"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// memoryFacetStore is an in-memory FacetStore for testing.
type memoryFacetStore struct {
	mu     sync.Mutex
	facets map[string]*SessionFacet
}

func newMemoryFacetStore() *memoryFacetStore {
	return &memoryFacetStore{facets: map[string]*SessionFacet{}}
}

func (s *memoryFacetStore) SaveFacet(_ context.Context, f *SessionFacet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.facets[f.SessionID] = f
	return nil
}

func (s *memoryFacetStore) GetFacet(_ context.Context, sessionID string) (*SessionFacet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.facets[sessionID]; ok {
		return f, nil
	}
	return nil, errors.New("not found")
}

func (s *memoryFacetStore) ListFacets(_ context.Context) ([]*SessionFacet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*SessionFacet, 0, len(s.facets))
	for _, f := range s.facets {
		result = append(result, f)
	}
	return result, nil
}

// mockQuerierWithSessions returns an overview with the given session IDs.
type mockQuerierWithSessions struct {
	mockQuerier
	sessions []SessionSummary
}

func (m *mockQuerierWithSessions) Overview(_ context.Context, _ Filters) (*Overview, error) {
	return &Overview{Sessions: m.sessions}, nil
}

var _ = Describe("FacetWorker", func() {
	It("processes sessions that have no existing facets", func() {
		store := newMemoryFacetStore()

		sessions := []SessionSummary{
			{ID: "s1", Label: "Fix bug"},
			{ID: "s2", Label: "Add feature"},
			{ID: "s3", Label: "Refactor"},
		}

		// Pre-fill one session
		store.facets["s2"] = &SessionFacet{SessionID: "s2", GoalCategory: "implement_feature"}

		detail := &SessionDetail{
			Summary:  SessionSummary{ID: "test"},
			Messages: []SessionMessage{{Role: "user", Text: "test"}},
		}

		mockLLM := func(_ context.Context, _ string) (string, error) {
			return `{
				"underlying_goal": "test goal",
				"goal_category": "fix_bug",
				"outcome": "fully_achieved",
				"session_type": "single_task",
				"friction_types": [],
				"brief_summary": "test summary"
			}`, nil
		}

		querier := &mockQuerierWithSessions{
			mockQuerier: mockQuerier{detail: detail},
			sessions:    sessions,
		}

		extractor := NewFacetExtractor(querier, mockLLM, store)
		worker := NewFacetWorker(extractor, store, querier)

		worker.Run(context.Background())

		done, total := worker.Progress()
		Expect(total).To(Equal(2)) // s1 and s3 needed processing, s2 was skipped
		Expect(done).To(Equal(2))

		// All sessions should now have facets
		_, err := store.GetFacet(context.Background(), "s1")
		Expect(err).NotTo(HaveOccurred())
		_, err = store.GetFacet(context.Background(), "s2")
		Expect(err).NotTo(HaveOccurred())
		_, err = store.GetFacet(context.Background(), "s3")
		Expect(err).NotTo(HaveOccurred())
	})

	It("reports zero progress with no sessions", func() {
		store := newMemoryFacetStore()
		querier := &mockQuerierWithSessions{sessions: nil}
		extractor := NewFacetExtractor(querier, nil, store)
		worker := NewFacetWorker(extractor, store, querier)

		worker.Run(context.Background())

		done, total := worker.Progress()
		Expect(total).To(Equal(0))
		Expect(done).To(Equal(0))
	})

	It("respects context cancellation", func() {
		store := newMemoryFacetStore()
		sessions := make([]SessionSummary, 50)
		for i := range sessions {
			sessions[i] = SessionSummary{ID: fmt.Sprintf("s%d", i)}
		}

		detail := &SessionDetail{
			Messages: []SessionMessage{{Role: "user", Text: "test"}},
		}

		// Slow LLM to ensure cancellation kicks in
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		mockLLM := func(ctx context.Context, _ string) (string, error) {
			return `{"underlying_goal":"t","goal_category":"fix_bug","outcome":"fully_achieved","session_type":"single_task","friction_types":[],"brief_summary":"t"}`, ctx.Err()
		}

		querier := &mockQuerierWithSessions{
			mockQuerier: mockQuerier{detail: detail},
			sessions:    sessions,
		}
		extractor := NewFacetExtractor(querier, mockLLM, store)
		worker := NewFacetWorker(extractor, store, querier)

		worker.Run(ctx)

		// Should have processed very few or no sessions due to immediate cancellation
		done, _ := worker.Progress()
		Expect(done).To(BeNumerically("<", 50))
	})
})
