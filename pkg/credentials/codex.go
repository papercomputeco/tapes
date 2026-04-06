package credentials

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// ReadCodexAuthFile reads ~/.codex/auth.json and returns its contents and path.
// Returns nil, "" if the file cannot be read.
func ReadCodexAuthFile() ([]byte, string) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, ""
	}

	authPath := filepath.Join(home, ".codex", "auth.json")
	data, err := os.ReadFile(authPath)
	if err != nil {
		return nil, ""
	}

	return data, authPath
}

// HasCodexOAuthTokens returns true when the Codex auth JSON contains OAuth
// tokens from `codex login`.
func HasCodexOAuthTokens(data []byte) bool {
	var auth struct {
		Tokens *struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
		} `json:"tokens"`
	}
	if err := json.Unmarshal(data, &auth); err != nil {
		return false
	}
	if auth.Tokens == nil {
		return false
	}
	return auth.Tokens.AccessToken != "" || auth.Tokens.RefreshToken != ""
}

// PatchCodexAuthKey sets OPENAI_API_KEY in the codex auth JSON and returns the
// updated bytes. OAuth "tokens" are preserved so users who authenticated with
// `codex login` keep their native Codex OAuth flow available.
// Returns nil, false if the JSON cannot be processed.
func PatchCodexAuthKey(data []byte, apiKey string) ([]byte, bool) {
	var auth map[string]json.RawMessage
	if err := json.Unmarshal(data, &auth); err != nil {
		return nil, false
	}

	keyJSON, err := json.Marshal(apiKey)
	if err != nil {
		return nil, false
	}
	auth["OPENAI_API_KEY"] = keyJSON

	updated, err := json.MarshalIndent(auth, "", "  ")
	if err != nil {
		return nil, false
	}

	return updated, true
}
