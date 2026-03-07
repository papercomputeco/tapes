package provider

import (
	"fmt"

	"github.com/papercomputeco/tapes/pkg/llm/provider/anthropic"
	"github.com/papercomputeco/tapes/pkg/llm/provider/bedrock"
	"github.com/papercomputeco/tapes/pkg/llm/provider/ollama"
	"github.com/papercomputeco/tapes/pkg/llm/provider/openai"
)

// Supported provider type constants
const (
	Anthropic = "anthropic"
	Bedrock   = "bedrock"
	OpenAI    = "openai"
	Ollama    = "ollama"
)

// SupportedProviders returns the list of all supported provider type names.
func SupportedProviders() []string {
	return []string{Anthropic, Bedrock, OpenAI, Ollama}
}

// New creates a new Provider instance for the given provider type.
// Returns an error if the provider type is not recognized.
func New(providerType string) (Provider, error) {
	switch providerType {
	case Anthropic:
		return anthropic.New(), nil
	case Bedrock:
		return bedrock.New(), nil
	case OpenAI:
		return openai.New(), nil
	case Ollama:
		return ollama.New(), nil
	default:
		return nil, fmt.Errorf("unknown provider type: %q (supported: %v)", providerType, SupportedProviders())
	}
}
