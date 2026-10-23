package extproc

import (
	"encoding/base64"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"math"
	"regexp"
)

const (
	ExecutionReceiptHeader   = "x-paper-execution-receipt"
	SelectionMetadataHeader  = "x-vsr-selection"
	MaxExecutionReceiptBytes = 8 << 10
)

const (
	settingForwarded             = "forwarded"
	finalForwardedBodyProvenance = "final-forwarded-body"
)

// SelectionMetadataV1 is the generic router's request-leased selection. Backend
// and Model describe router intent; they never establish the actual dispatch.
type SelectionMetadataV1 struct {
	// Recipe context is an all-or-none extension. It is copied from the leased
	// router generation, never reconstructed from the latest control-plane state.
	Shape                 string `json:"shape,omitempty"`
	Recipe                string `json:"recipe,omitempty"`
	ConfigurationID       string `json:"configurationId,omitempty"`
	ConfigurationRevision string `json:"configurationRevision,omitempty"`
	BackendName           string `json:"backendName,omitempty"`
	BackendGeneration     string `json:"backendGeneration,omitempty"`
	Version               int    `json:"version"`
	ConfigHash            string `json:"configHash"`
	Decision              string `json:"decision"`
	DestinationID         string `json:"destinationId"`
	Model                 string `json:"model,omitempty"`
	Backend               string `json:"backend,omitempty"`
	Protocol              string `json:"protocol,omitempty"`
}

// ReceiptSetting describes bytes sent to the provider. Absent values do not
// reveal provider defaults or whether an earlier transform removed a value.
type ReceiptSetting struct {
	State      string `json:"state"`
	Value      any    `json:"value,omitempty"`
	Provenance string `json:"provenance"`
}

// ExecutionReceiptV1 is an initial, bounded dispatch report. It contains neither
// prompts nor credentials. The downstream stream adapter owns terminal outcome;
// receiving headers does not prove successful inference or stream completion.
type ExecutionReceiptV1 struct {
	Version       int                       `json:"version"`
	ReceiptID     string                    `json:"receiptId"`
	AttemptID     string                    `json:"attemptId"`
	RequestID     string                    `json:"requestId"`
	GatewayID     string                    `json:"gatewayId"`
	Selection     SelectionMetadataV1       `json:"selection"`
	BackendID     string                    `json:"backendId,omitempty"`
	Model         string                    `json:"model,omitempty"`
	Settings      map[string]ReceiptSetting `json:"settings"`
	Outcome       string                    `json:"outcome"`
	FailureStage  string                    `json:"failureStage,omitempty"`
	TerminalOwner string                    `json:"terminalOwner"`
}

var (
	receiptIdentifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:/@+\-]{0,255}$`)
	receiptHash       = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

func validReceiptID(value string) bool { return receiptIdentifier.MatchString(value) }

func parseSelection(value string) *SelectionMetadataV1 {
	if len(value) == 0 || len(value) > 5500 {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) > 4096 {
		return nil
	}
	// The v2 decoder rejects duplicate names and case-folded identity aliases.
	// Explicit presence also distinguishes an omitted legacy context from a
	// malformed context whose fields were supplied as empty strings or null.
	var fields map[string]json.RawMessage
	if jsonv2.Unmarshal(raw, &fields) != nil {
		return nil
	}
	var selection SelectionMetadataV1
	if jsonv2.Unmarshal(raw, &selection, jsonv2.RejectUnknownMembers(true)) != nil || selection.Version != 1 || !receiptHash.MatchString(selection.ConfigHash) || !validReceiptID(selection.DestinationID) || !selection.validRecipeContext(fields) {
		return nil
	}
	for _, value := range []string{selection.Model, selection.Backend, selection.Protocol} {
		if value != "" && !validReceiptID(value) {
			return nil
		}
	}
	return &selection
}

func (selection SelectionMetadataV1) hasRecipeContext() bool { return selection.Recipe != "" }

func (selection SelectionMetadataV1) validRecipeContext(fields map[string]json.RawMessage) bool {
	if decision, exists := fields["decision"]; !exists || len(decision) == 0 || decision[0] != '"' {
		return false
	}
	contextFields := []string{"shape", "recipe", "configurationId", "configurationRevision", "backendName", "backendGeneration"}
	present := 0
	for _, name := range contextFields {
		if _, exists := fields[name]; exists {
			present++
		}
	}
	if present == 0 {
		return validReceiptID(selection.Decision)
	}
	if present != len(contextFields) {
		return false
	}
	for _, value := range []string{selection.Recipe, selection.ConfigurationID, selection.ConfigurationRevision, selection.BackendName, selection.BackendGeneration} {
		if !validReceiptID(value) {
			return false
		}
	}
	switch selection.Shape {
	case "frog", "rabbit", "fox", "dragon":
	default:
		return false
	}
	return selection.Decision == "" || validReceiptID(selection.Decision)
}

// EncodeExecutionReceipt bounds the header itself, including base64 expansion.
func EncodeExecutionReceipt(receipt *ExecutionReceiptV1) (string, error) {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return "", err
	}
	if base64.RawURLEncoding.EncodedLen(len(raw)) > MaxExecutionReceiptBytes {
		return "", errors.New("execution receipt exceeds header limit")
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func forwardedSettings(body []byte) (string, map[string]ReceiptSetting) {
	// Only selected scalar paths are inspected. Never copy nested objects or
	// arbitrary string values: reasoning may carry signed/opaque private state.
	var request map[string]json.RawMessage
	if json.Unmarshal(body, &request) != nil {
		return "", nil
	}
	var model string
	_ = json.Unmarshal(request["model"], &model)
	if !validReceiptID(model) {
		return "", nil
	}
	settings := make(map[string]ReceiptSetting)
	number := func(path string, raw json.RawMessage, maximum float64, integral bool) {
		setting := ReceiptSetting{State: "omitted-provider-default", Provenance: finalForwardedBodyProvenance}
		if len(raw) != 0 {
			var value float64
			if json.Unmarshal(raw, &value) == nil && value >= 0 && value <= maximum && (!integral || math.Trunc(value) == value) && string(raw) != "null" {
				setting.State, setting.Value = settingForwarded, value
			} else {
				setting.State = labelUnknown
			}
		}
		settings[path] = setting
	}
	number("temperature", request["temperature"], 2, false)
	number("top_p", request["top_p"], 1, false)
	for _, key := range []string{"max_tokens", "max_completion_tokens", "max_output_tokens"} {
		number(key, request[key], 1e9, true)
	}
	var stream bool
	streamState := ReceiptSetting{State: "omitted-provider-default", Provenance: finalForwardedBodyProvenance}
	if raw := request["stream"]; len(raw) > 0 {
		if json.Unmarshal(raw, &stream) == nil && string(raw) != "null" {
			streamState.State, streamState.Value = settingForwarded, stream
		} else {
			streamState.State = labelUnknown
		}
	}
	settings["stream"] = streamState
	effort := func(key string, raw json.RawMessage) {
		setting := ReceiptSetting{State: "omitted-provider-default", Provenance: finalForwardedBodyProvenance}
		if len(raw) != 0 {
			var value string
			_ = json.Unmarshal(raw, &value)
			switch value {
			case "none", "minimal", "low", "medium", "high", "xhigh", "max":
				setting.State, setting.Value = settingForwarded, value
			default:
				setting.State = labelUnknown
			}
		}
		settings[key] = setting
	}
	effort("reasoning_effort", request["reasoning_effort"])
	var reasoning, thinking map[string]json.RawMessage
	reasoningErr := json.Unmarshal(request["reasoning"], &reasoning)
	thinkingErr := json.Unmarshal(request["thinking"], &thinking)
	effort("reasoning.effort", reasoning["effort"])
	number("thinking.budget_tokens", thinking["budget_tokens"], 1e9, true)
	kind := ReceiptSetting{State: "omitted-provider-default", Provenance: finalForwardedBodyProvenance}
	if raw := thinking["type"]; len(raw) > 0 {
		var value string
		_ = json.Unmarshal(raw, &value)
		switch value {
		case "enabled", "disabled", "adaptive":
			kind.State, kind.Value = settingForwarded, value
		default:
			kind.State = labelUnknown
		}
	}
	settings["thinking.type"] = kind
	// A present but unreadable parent does not establish that its fields were
	// omitted. Do not reflect the invalid value into the receipt.
	if _, present := request["reasoning"]; present && (reasoningErr != nil || reasoning == nil) {
		settings["reasoning.effort"] = ReceiptSetting{State: labelUnknown, Provenance: finalForwardedBodyProvenance}
	}
	if _, present := request["thinking"]; present && (thinkingErr != nil || thinking == nil) {
		for _, key := range []string{"thinking.type", "thinking.budget_tokens"} {
			settings[key] = ReceiptSetting{State: labelUnknown, Provenance: finalForwardedBodyProvenance}
		}
	}
	return model, settings
}
