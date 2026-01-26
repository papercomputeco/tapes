package provider

import (
	"fmt"

	"github.com/papercomputeco/tapes/pkg/llm/provider/anthropic"
	"github.com/papercomputeco/tapes/pkg/llm/provider/besteffort"
	"github.com/papercomputeco/tapes/pkg/llm/provider/ollama"
	"github.com/papercomputeco/tapes/pkg/llm/provider/openai"
)

// Supported provider type constants
const (
	Anthropic  = "anthropic"
	OpenAI     = "openai"
	Ollama     = "ollama"
	BestEffort = "besteffort"
)

// SupportedProviders returns the list of all supported provider type names.
func SupportedProviders() []string {
	return []string{Anthropic, OpenAI, Ollama, BestEffort}
}

// New creates a new Provider instance for the given provider type.
// Returns an error if the provider type is not recognized.
func New(providerType string) (Provider, error) {
	switch providerType {
	case Anthropic:
		return anthropic.New(), nil
	case OpenAI:
		return openai.New(), nil
	case Ollama:
		return ollama.New(), nil
	case BestEffort:
		return besteffort.New(), nil
	default:
		return nil, fmt.Errorf("unknown provider type: %q (supported: %v)", providerType, SupportedProviders())
	}
}
