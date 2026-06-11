package proxy

import "strings"

// LocalProviderUpstreams returns provider-specific upstreams that make the
// proxy usable as a local OpenAI-compatible front door when the default stack is
// Ollama. Ollama exposes OpenAI-compatible routes under /v1, while Tapes' native
// Ollama provider still uses the root upstream for /api/chat.
func LocalProviderUpstreams(providerType, upstream string) map[string]string {
	if providerType != providerOllama || strings.TrimSpace(upstream) == "" {
		return nil
	}
	trimmed := strings.TrimRight(strings.TrimSpace(upstream), "/")
	return map[string]string{
		providerOpenAI: trimmed + "/v1",
		providerOllama: trimmed,
	}
}
