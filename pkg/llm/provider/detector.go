// Package provider
package provider

import (
	"github.com/papercomputeco/tapes/pkg/llm/provider/anthropic"
	"github.com/papercomputeco/tapes/pkg/llm/provider/besteffort"
	"github.com/papercomputeco/tapes/pkg/llm/provider/ollama"
	"github.com/papercomputeco/tapes/pkg/llm/provider/openai"
)

// Detector manages provider detection by checking registered providers in order.
type Detector struct {
	providers []Provider
}

// NewDetector creates a new Detector with the default set of providers.
// Providers are checked in order: Anthropic, OpenAI, Ollama, then BestEffort as fallback.
func NewDetector() *Detector {
	return &Detector{
		providers: []Provider{
			anthropic.New(),
			openai.New(),
			ollama.New(),
		},
	}
}

// Detect returns the appropriate provider for the given payload.
// It iterates through registered providers and returns the first one
// that reports it can handle the payload. If no provider matches,
// BestEffort is returned as the fallback.
func (d *Detector) Detect(payload []byte) Provider {
	for _, p := range d.providers {
		if p.CanHandle(payload) {
			return p
		}
	}
	return besteffort.New()
}

// DetectRequest is a convenience method that detects the provider
// and parses the request in one call.
func (d *Detector) DetectRequest(payload []byte) (Provider, error) {
	p := d.Detect(payload)
	_, err := p.ParseRequest(payload)
	return p, err
}
