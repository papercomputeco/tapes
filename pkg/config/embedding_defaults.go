package config

import "strings"

const (
	defaultOpenAIEmbeddingTarget     = "https://api.openai.com"
	defaultOpenAIEmbeddingModel      = "text-embedding-3-large"
	defaultOpenAIEmbeddingDimensions = 1024

	legacyOllamaEmbeddingModel = "nomic-embed-text"
)

// ResolveEmbeddingConfigOptions describes which values came from explicit
// user input instead of inherited defaults.
type ResolveEmbeddingConfigOptions struct {
	DimensionsSet bool
}

// ResolveEmbeddingConfig normalizes provider-specific embedding defaults after
// config, flags, and environment variables have been merged.
func ResolveEmbeddingConfig(provider, target, model string, dimensions uint) EmbeddingConfig {
	return ResolveEmbeddingConfigWithOptions(provider, target, model, dimensions, ResolveEmbeddingConfigOptions{})
}

// ResolveEmbeddingConfigWithOptions normalizes provider-specific embedding
// defaults while preserving explicitly configured values.
func ResolveEmbeddingConfigWithOptions(provider, target, model string, dimensions uint, opts ResolveEmbeddingConfigOptions) EmbeddingConfig {
	switch strings.ToLower(provider) {
	case "openai":
		inheritedLocalTarget := target == "" || target == defaultEmbeddingTarget || target == defaultUpstream
		inheritedLocalModel := model == "" || model == defaultEmbeddingModel || model == legacyOllamaEmbeddingModel
		inheritedLocalDimensions := dimensions == 0 || (!opts.DimensionsSet && dimensions == defaultEmbeddingDimensions && (inheritedLocalTarget || inheritedLocalModel))

		if inheritedLocalTarget {
			target = defaultOpenAIEmbeddingTarget
		}
		if inheritedLocalModel {
			model = defaultOpenAIEmbeddingModel
		}
		if inheritedLocalDimensions {
			dimensions = defaultOpenAIEmbeddingDimensions
		}

		return EmbeddingConfig{
			Provider:   "openai",
			Target:     target,
			Model:      model,
			Dimensions: dimensions,
		}

	case "ollama":
		if target == "" {
			target = defaultEmbeddingTarget
		}
		if model == "" || model == legacyOllamaEmbeddingModel {
			model = defaultEmbeddingModel
		}
		if dimensions == 0 {
			dimensions = defaultEmbeddingDimensions
		}

		return EmbeddingConfig{
			Provider:   "ollama",
			Target:     target,
			Model:      model,
			Dimensions: dimensions,
		}

	default:
		return EmbeddingConfig{
			Provider:   provider,
			Target:     target,
			Model:      model,
			Dimensions: dimensions,
		}
	}
}
