// Package agenttrace defines domain types for the Agent Trace specification,
// an open specification for tracking AI-generated code attribution.
package agenttrace

// AgentTrace is the root record for an agent trace.
type AgentTrace struct {
	Version   string         `json:"version"`
	ID        string         `json:"id"`
	Timestamp string         `json:"timestamp"`
	VCS       *VCS           `json:"vcs,omitempty"`
	Tool      *Tool          `json:"tool,omitempty"`
	Files     []File         `json:"files"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// VCS describes the version control system context.
type VCS struct {
	Type     string `json:"type,omitempty"`
	Revision string `json:"revision,omitempty"`
}

// Tool describes the tool that generated the trace.
type Tool struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// File describes a file with AI-attributed conversations.
type File struct {
	Path          string         `json:"path"`
	Conversations []Conversation `json:"conversations,omitempty"`
}

// Conversation describes a conversation that contributed to a file.
type Conversation struct {
	URL              string            `json:"url,omitempty"`
	Contributor      *Contributor      `json:"contributor,omitempty"`
	Ranges           []Range           `json:"ranges,omitempty"`
	RelatedResources []RelatedResource `json:"related_resources,omitempty"`
}

// Contributor describes who contributed to the code (AI or human).
type Contributor struct {
	Type    string `json:"type,omitempty"`
	ModelID string `json:"model_id,omitempty"`
}

// Range describes a range of lines attributed to AI generation.
type Range struct {
	StartLine   int          `json:"start_line"`
	EndLine     int          `json:"end_line"`
	ContentHash string       `json:"content_hash,omitempty"`
	Contributor *Contributor `json:"contributor,omitempty"`
}

// RelatedResource describes a resource related to the conversation.
type RelatedResource struct {
	Type string `json:"type,omitempty"`
	URL  string `json:"url,omitempty"`
}
