// Package ollama
package ollama

import "time"

// ollamaRequest represents Ollama's request format.
type ollamaRequest struct {
	Model     string          `json:"model"`
	Messages  []ollamaMessage `json:"messages"`
	Stream    *bool           `json:"stream,omitempty"`
	Format    string          `json:"format,omitempty"`
	KeepAlive string          `json:"keep_alive,omitempty"`
	Options   *ollamaOptions  `json:"options,omitempty"`
}

type ollamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`

	// Base64-encoded images
	Images []string `json:"images,omitempty"`

	// Tool calls (assistant requesting tool execution)
	ToolCalls []ollamaToolCall `json:"tool_calls,omitempty"`
}

type ollamaToolCall struct {
	ID       string `json:"id"`
	Function struct {
		Index     int            `json:"index,omitempty"`
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	} `json:"function"`
}

type ollamaOptions struct {
	Temperature   *float64 `json:"temperature,omitempty"`
	TopP          *float64 `json:"top_p,omitempty"`
	TopK          *int     `json:"top_k,omitempty"`
	Seed          *int     `json:"seed,omitempty"`
	NumPredict    *int     `json:"num_predict,omitempty"`
	NumCtx        *int     `json:"num_ctx,omitempty"`
	RepeatPenalty *float64 `json:"repeat_penalty,omitempty"`
	RepeatLastN   *int     `json:"repeat_last_n,omitempty"`
	Stop          []string `json:"stop,omitempty"`
}

// ollamaResponse represents Ollama's response format.
type ollamaResponse struct {
	Model              string        `json:"model"`
	CreatedAt          time.Time     `json:"created_at"`
	Message            ollamaMessage `json:"message"`
	Done               bool          `json:"done"`
	DoneReason         string        `json:"done_reason,omitempty"`
	Context            []int         `json:"context,omitempty"`
	TotalDuration      int64         `json:"total_duration,omitempty"`
	LoadDuration       int64         `json:"load_duration,omitempty"`
	PromptEvalCount    int           `json:"prompt_eval_count,omitempty"`
	PromptEvalDuration int64         `json:"prompt_eval_duration,omitempty"`
	EvalCount          int           `json:"eval_count,omitempty"`
	EvalDuration       int64         `json:"eval_duration,omitempty"`
}
