package credentials

// Credentials represents the stored API credentials in credentials.toml.
type Credentials struct {
	Version   int                           `toml:"version"`
	Providers map[string]ProviderCredential `toml:"providers"`
}

// ProviderCredential holds the API key for a single provider.
type ProviderCredential struct {
	APIKey string `toml:"api_key"`
}
