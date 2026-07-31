package sqlitecore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

var recoveryReducers = map[string]capture.Reducer{
	capture.ProviderAnthropic: capture.NewAnthropicReducer(),
	capture.ProviderOpenAI:    capture.NewOpenAIResponsesReducer(),
}

// recoverReduction lets a later build repair a capture-time reduction failure
// from the verbatim response retained in the raw layer. It is best effort: an
// unsupported provider or malformed payload remains raw-only, never failing a
// whole session derive.
func recoverReduction(ctx context.Context, rec *storage.RawTurnRecord) {
	if len(rec.RawResponse) == 0 || !reducedResponseUnusable(rec.Response) {
		return
	}
	reducer, ok := recoveryReducers[rec.Provider]
	if !ok {
		return
	}
	body, err := decodeStoredEncoding(rec.RawResponse, rec.RawResponseEncoding)
	if err != nil {
		return
	}
	response, err := reducer.Reduce(ctx, bytes.NewReader(rec.RawRequest), bytes.NewReader(body), contentTypeFromMeta(rec.Meta))
	if err != nil || response == nil {
		return
	}
	out, err := json.Marshal(response)
	if err == nil && !reducedResponseUnusable(out) {
		rec.Response = out
	}
}

func reducedResponseUnusable(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var response llm.ChatResponse
	if json.Unmarshal(raw, &response) != nil {
		return true
	}
	return response.Message.Role == "" || len(response.Message.Content) == 0
}

func decodeStoredEncoding(body []byte, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return body, nil
	case "gzip", "x-gzip":
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	default:
		return nil, unsupportedEncodingError(encoding)
	}
}

type unsupportedEncodingError string

func (e unsupportedEncodingError) Error() string { return "unsupported content-encoding " + string(e) }

func contentTypeFromMeta(meta json.RawMessage) string {
	var value struct {
		ContentType string `json:"content_type"`
	}
	if json.Unmarshal(meta, &value) != nil {
		return ""
	}
	return value.ContentType
}
