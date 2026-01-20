package llm

// ConversationTurn represents a complete request-response pair for storage in the DAG.
type ConversationTurn struct {
	Provider string        `json:"provider"`
	Request  *ChatRequest  `json:"request"`
	Response *ChatResponse `json:"response"`
}
