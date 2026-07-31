package v1alpha1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

// Manifest is a cassette author's declaration of identity, dependencies, API
// anchors, owned tables, and configuration surface.
type Manifest struct {
	Kind         string          `json:"kind"`
	Cassette     Identity        `json:"cassette"`
	Depends      Depends         `json:"depends"`
	API          APIAnchors      `json:"api"`
	Tables       []Table         `json:"tables,omitempty"`
	Config       []Setting       `json:"config,omitempty"`
	SourceDigest cassette.Digest `json:"x-source-digest,omitempty"`
}

// Identity describes a cassette release.
type Identity struct {
	Name        cassette.Name    `json:"name"`
	Version     cassette.Version `json:"version"`
	DisplayName string           `json:"display_name,omitempty"`
	Description string           `json:"description,omitempty"`
	License     string           `json:"license,omitempty"`
	Homepage    string           `json:"homepage,omitempty"`
	Image       string           `json:"image,omitempty"`
	Port        int              `json:"port,omitempty"`
}

// Depends declares the tapes contract and views read by a cassette.
type Depends struct {
	Core  cassette.ContractVersion `json:"core"`
	Views []string                 `json:"views,omitempty"`
}

// APIAnchors are paths on the running cassette's listener.
type APIAnchors struct {
	Health  string `json:"health"`
	OpenAPI string `json:"openapi"`

	// Prefix is the path the cassette mounts its own API beneath, declared
	// without slashes ("api"). Core serves that API at /v1/cassettes/<name>,
	// so this field is what tells core which head to swap for that one.
	//
	// It exists because a cassette should be able to serve a sensible path on
	// its own listener — /api/<name>/... reads correctly when you curl the
	// process directly — without core's public namespace leaking into the
	// cassette's own routing table. Declaring the prefix keeps the mapping a
	// single stated segment rather than a guess.
	//
	// Defaults to "api". The literal "/" means the cassette mounts directly
	// under its own name and there is nothing to strip.
	Prefix string `json:"prefix_path"`
}

// PrefixPath returns the normalized prefix path: surrounding slashes removed,
// so "/" and "" both mean no prefix at all. The raw field keeps whatever the
// author wrote, because that is what the canonical form and therefore the
// manifest digest are computed over.
func (anchors APIAnchors) PrefixPath() string {
	return strings.Trim(anchors.Prefix, "/")
}

// Table names a table owned by the cassette in its derived schema.
type Table struct {
	Name string `json:"name"`
}

// Parse strictly decodes exactly one JSON cassette metadata object and applies
// defaults. It checks kind before strict decoding so dispatch errors remain
// useful when a document belongs to another schema version.
func Parse(data []byte) (*Manifest, error) {
	if err := rejectDuplicateMetadataKeys(data); err != nil {
		return nil, fmt.Errorf("decode cassette metadata: %w", err)
	}
	var header struct {
		Kind string `json:"kind"`
	}
	headerDecoder := json.NewDecoder(bytes.NewReader(data))
	headerDecoder.UseNumber()
	if err := headerDecoder.Decode(&header); err != nil {
		return nil, fmt.Errorf("decode cassette metadata header: %w", err)
	}
	if header.Kind != Kind {
		return nil, fmt.Errorf("expected kind %q, got %q", Kind, header.Kind)
	}
	if err := validateMetadataKeys(data); err != nil {
		return nil, err
	}

	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode cassette metadata: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("decode cassette metadata: trailing JSON value")
		}
		return nil, fmt.Errorf("decode cassette metadata trailing data: %w", err)
	}

	ApplyDefaults(&manifest)
	return &manifest, nil
}

func rejectDuplicateMetadataKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := consumeMetadataJSONValue(decoder); err != nil {
		return err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing token %v", token)
		}
		return err
	}
	return nil
}

func consumeMetadataJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeMetadataJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := consumeMetadataJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("unexpected delimiter %q", delimiter)
	}
}

func validateMetadataKeys(data []byte) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("decode cassette metadata: %w", err)
	}
	if err := checkObjectKeys(root, "", "kind", "cassette", "depends", "api", "tables", "config", "x-source-digest"); err != nil {
		return err
	}
	if err := checkNestedObject(root["cassette"], "cassette", "name", "version", "display_name", "description", "license", "homepage", "image", "port"); err != nil {
		return err
	}
	if err := checkNestedObject(root["depends"], "depends", "core", "views"); err != nil {
		return err
	}
	if err := checkNestedObject(root["api"], "api", "health", "openapi", "prefix_path"); err != nil {
		return err
	}
	if err := checkArrayObjects(root["tables"], "tables", []string{"name"}); err != nil {
		return err
	}
	return checkArrayObjects(root["config"], "config", []string{
		"key", "type", "required", "default", "secret", "description", "enum", "min", "max",
	})
}

func checkNestedObject(raw json.RawMessage, path string, allowed ...string) error {
	if len(raw) == 0 || string(raw) == jsonNull {
		return nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return fmt.Errorf("decode cassette metadata %s object: %w", path, err)
	}
	return checkObjectKeys(object, path, allowed...)
}

func checkArrayObjects(raw json.RawMessage, path string, allowed []string) error {
	if len(raw) == 0 || string(raw) == jsonNull {
		return nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return fmt.Errorf("decode cassette metadata %s array: %w", path, err)
	}
	for index, item := range items {
		if err := checkNestedObject(item, fmt.Sprintf("%s[%d]", path, index), allowed...); err != nil {
			return err
		}
	}
	return nil
}

func checkObjectKeys(object map[string]json.RawMessage, path string, allowed ...string) error {
	accepted := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		accepted[key] = struct{}{}
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := accepted[key]; ok {
			continue
		}
		field := key
		if path != "" {
			field = path + "." + key
		}
		return fmt.Errorf("decode cassette metadata: unknown field %q", field)
	}
	return nil
}

// ApplyDefaults applies all cassette metadata defaults in one place.
func ApplyDefaults(manifest *Manifest) {
	if manifest == nil {
		return
	}
	if manifest.API.Health == "" {
		manifest.API.Health = "/ping"
	}
	if manifest.API.OpenAPI == "" {
		manifest.API.OpenAPI = "/openapi"
	}
	if manifest.API.Prefix == "" {
		manifest.API.Prefix = DefaultPrefixPath
	}
}

// MarshalCanonical returns deterministic JSON for identity and hashing. All
// set-like arrays are sorted without mutating the manifest.
func (m *Manifest) MarshalCanonical() ([]byte, error) {
	if m == nil {
		return nil, errors.New("marshal canonical cassette manifest: nil manifest")
	}

	normalized := *m
	ApplyDefaults(&normalized)
	m = &normalized

	root := map[string]any{
		"api": map[string]any{
			"health":      m.API.Health,
			"openapi":     m.API.OpenAPI,
			"prefix_path": m.API.Prefix,
		},
		"cassette": canonicalIdentity(m.Cassette),
		"depends":  canonicalDepends(m.Depends),
		"kind":     m.Kind,
	}
	if m.SourceDigest != "" {
		root["x-source-digest"] = m.SourceDigest
	}
	if len(m.Tables) > 0 {
		tables := append([]Table(nil), m.Tables...)
		sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
		root["tables"] = tables
	}
	if len(m.Config) > 0 {
		settings := append([]Setting(nil), m.Config...)
		sort.Slice(settings, func(i, j int) bool { return settings[i].Key < settings[j].Key })
		canonical := make([]map[string]any, 0, len(settings))
		for _, setting := range settings {
			canonical = append(canonical, canonicalSetting(setting))
		}
		root["config"] = canonical
	}

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(root); err != nil {
		return nil, fmt.Errorf("marshal canonical cassette manifest: %w", err)
	}

	return bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), nil
}

func canonicalIdentity(identity Identity) map[string]any {
	value := map[string]any{
		"name":    identity.Name,
		"version": identity.Version,
	}
	putString(value, "description", identity.Description)
	putString(value, "display_name", identity.DisplayName)
	putString(value, "homepage", identity.Homepage)
	putString(value, "image", identity.Image)
	putString(value, "license", identity.License)
	if identity.Port != 0 {
		value["port"] = identity.Port
	}

	return value
}

func canonicalDepends(depends Depends) map[string]any {
	value := map[string]any{"core": depends.Core}
	if len(depends.Views) > 0 {
		views := append([]string(nil), depends.Views...)
		sort.Strings(views)
		value["views"] = views
	}

	return value
}

func canonicalSetting(setting Setting) map[string]any {
	value := map[string]any{
		"key":  setting.Key,
		"type": setting.Type,
	}
	if setting.Required {
		value["required"] = true
	}
	if setting.Default != nil {
		defaultValue := setting.Default
		if setting.Type == SettingTypeInt {
			if integer, ok := integerValue(setting.Default); ok {
				defaultValue = integer
			}
		}
		value["default"] = defaultValue
	}
	if setting.Secret {
		value["secret"] = true
	}
	putString(value, "description", setting.Description)
	if len(setting.Enum) > 0 {
		enum := append([]string(nil), setting.Enum...)
		sort.Strings(enum)
		value["enum"] = enum
	}
	if setting.Min != nil {
		value["min"] = *setting.Min
	}
	if setting.Max != nil {
		value["max"] = *setting.Max
	}

	return value
}

func putString(object map[string]any, key, value string) {
	if value != "" {
		object[key] = value
	}
}

// Digest returns the SHA-256 digest of the canonical JSON representation.
func (m *Manifest) Digest() (cassette.Digest, error) {
	canonical, err := m.MarshalCanonical()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)

	return cassette.Digest("sha256:" + hex.EncodeToString(sum[:])), nil
}

// Redact returns a deep copy with defaults removed from secret settings.
func (m *Manifest) Redact() *Manifest {
	if m == nil {
		return nil
	}

	redacted := *m
	redacted.Depends.Views = append([]string(nil), m.Depends.Views...)
	redacted.Tables = append([]Table(nil), m.Tables...)
	redacted.Config = make([]Setting, len(m.Config))
	for index, setting := range m.Config {
		redacted.Config[index] = setting
		redacted.Config[index].Enum = append([]string(nil), setting.Enum...)
		if setting.Secret {
			redacted.Config[index].Default = nil
		}
	}

	return &redacted
}
