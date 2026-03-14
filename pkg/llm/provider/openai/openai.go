// Package openai
package openai

import (
	"github.com/papercomputeco/tapes/pkg/llm"
)

// Provider implements the Provider interface for OpenAI's Chat Completions API.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (o *Provider) Name() string {
	return "openai"
}

// DefaultStreaming is false - OpenAI requires explicit "stream": true.
func (o *Provider) DefaultStreaming() bool {
	return false
}

func (o *Provider) ParseRequest(payload []byte) (*llm.ChatRequest, error) {
	return ParseRequestPayload(payload)
}

func (o *Provider) ParseResponse(payload []byte) (*llm.ChatResponse, error) {
	return ParseResponsePayload(payload)
}

func (o *Provider) ParseStreamChunk(_ []byte) (*llm.StreamChunk, error) {
	panic("Not yet implemented")
}
