package v1alpha1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// SettingType is the scalar representation expected in deployment-provided
// cassette configuration.
type SettingType string

const (
	SettingTypeString   SettingType = "string"
	SettingTypeInt      SettingType = "int"
	SettingTypeBool     SettingType = "bool"
	SettingTypeDuration SettingType = "duration"
	SettingTypeJSON     SettingType = "json"
)

var settingTypeNames = []string{
	string(SettingTypeString),
	string(SettingTypeInt),
	string(SettingTypeBool),
	string(SettingTypeDuration),
	string(SettingTypeJSON),
}

// Setting declares one cassette configuration value. The deployment system
// supplies values; tapes publishes the schema but does not configure processes.
type Setting struct {
	Key string `json:"key"`

	// Env overrides the environment variable derived from Key.
	Env         string      `json:"env,omitempty"`
	Type        SettingType `json:"type"`
	Required    bool        `json:"required,omitempty"`
	Default     any         `json:"default,omitempty"`
	Secret      bool        `json:"secret,omitempty"`
	Description string      `json:"description,omitempty"`
	Enum        []string    `json:"enum,omitempty"`
	Min         *int64      `json:"min,omitempty"`
	Max         *int64      `json:"max,omitempty"`
}

// UnmarshalJSON preserves JSON integer defaults as json.Number so canonical
// JSON remains stable across a round trip, including beyond float64's exact
// integer range.
func (setting *Setting) UnmarshalJSON(data []byte) error {
	type settingJSON struct {
		Key         string          `json:"key"`
		Env         string          `json:"env"`
		Type        SettingType     `json:"type"`
		Required    bool            `json:"required"`
		Default     json.RawMessage `json:"default"`
		Secret      bool            `json:"secret"`
		Description string          `json:"description"`
		Enum        []string        `json:"enum"`
		Min         *int64          `json:"min"`
		Max         *int64          `json:"max"`
	}
	var decoded settingJSON
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("decode cassette setting: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("decode cassette setting: trailing JSON value")
		}
		return fmt.Errorf("decode cassette setting trailing data: %w", err)
	}

	*setting = Setting{
		Key:         decoded.Key,
		Env:         decoded.Env,
		Type:        decoded.Type,
		Required:    decoded.Required,
		Secret:      decoded.Secret,
		Description: decoded.Description,
		Enum:        decoded.Enum,
		Min:         decoded.Min,
		Max:         decoded.Max,
	}
	if len(decoded.Default) > 0 && string(decoded.Default) != jsonNull {
		defaultDecoder := json.NewDecoder(strings.NewReader(string(decoded.Default)))
		defaultDecoder.UseNumber()
		if err := defaultDecoder.Decode(&setting.Default); err != nil {
			return fmt.Errorf("decode cassette setting default: %w", err)
		}
	}

	return nil
}

func (settingType SettingType) valid() bool {
	switch settingType {
	case SettingTypeString, SettingTypeInt, SettingTypeBool, SettingTypeDuration, SettingTypeJSON:
		return true
	default:
		return false
	}
}

// EnvVar returns the declared environment variable, or derives one from the key.
func (setting *Setting) EnvVar() string {
	if setting.Env != "" {
		return setting.Env
	}

	return strings.ToUpper(strings.ReplaceAll(setting.Key, ".", "_"))
}
