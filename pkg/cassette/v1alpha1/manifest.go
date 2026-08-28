package v1alpha1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
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
	Publishes    *Publishes      `json:"publishes,omitempty"`
	Entities     []Entity        `json:"entities,omitempty"`
	Hooks        *Hooks          `json:"hooks,omitempty"`
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

	// Audience names the clients that should offer this cassette to a person —
	// "console", "paperctl", "tapesctl". It is presentation, not authorization:
	// core routes every installed cassette for anyone who can reach it, and a
	// client that ignores this field is not bypassing a control. What it buys is
	// a client not having to carry its own list of which cassettes belong in its
	// menu, which is the thing that goes stale.
	//
	// Omitted means every client, because that is what every manifest written
	// before this field existed meant. An audience is therefore only worth
	// declaring to *narrow* the set.
	//
	// The values are not a closed set. A cassette must be able to name a client
	// that shipped after the tapes release it validates against, so this checks
	// the shape of a name and not its membership — the cost being that a typo
	// reads as an unknown client rather than an error.
	Audience []string `json:"audience,omitempty"`
}

// ServesAudience reports whether a client called name should offer this
// cassette. The rule that an undeclared audience means everyone lives here so
// that no caller reimplements it — a client testing len(Audience) itself would
// hide every cassette that predates the field.
func (identity Identity) ServesAudience(name string) bool {
	if len(identity.Audience) == 0 {
		return true
	}

	return slices.Contains(identity.Audience, name)
}

// Depends declares the tapes contract and views read by a cassette.
type Depends struct {
	Core  cassette.ContractVersion `json:"core"`
	Views []string                 `json:"views,omitempty"`

	// Published names views other cassettes publish (schema-qualified, e.g.
	// "notes_v1.attachments") that this cassette reads. It is the consumer
	// side of the publishes mechanism, for genuinely static, same-license
	// dependencies — a deployment-configured consumer declares nothing here
	// and receives its view names as configuration instead. Like depends.views
	// this is a declaration only: the deployment owns the SELECT grant.
	Published []string `json:"published,omitempty"`
}

// Publishes declares what a cassette contributes back to core's read surface:
// views it maintains for others to join, and filter params it claims on core
// endpoints. This is the reverse direction of Depends — the mechanism that
// lets a cassette-owned fact become queryable inside core's own list SQL
// without core learning the cassette's semantics.
type Publishes struct {
	// Views are the schema-qualified views this cassette creates and
	// maintains. Core admits the declaration without database access and
	// never touches grants; provisioning SELECT for core's read role is
	// deployment-owned, symmetric with depends.views. A filter claim against
	// a published view only takes effect once core has probed the view as
	// readable with its own role — a separate arming pass, re-run on every
	// refresh, never part of admission.
	Views []string `json:"views,omitempty"`

	// Filters are the query params this cassette claims on core surfaces.
	Filters []FilterClaim `json:"filters,omitempty"`
}

// FilterClaim claims one query param on one core surface and carries
// everything core needs to execute the filter generically: which published
// view to probe, which rows of it satisfy the claim, and how to normalize
// supplied values before binding them. Core applies exactly this declaration
// and never cassette-specific grammar or semantics.
type FilterClaim struct {
	// Param is the claimed query param name. First claim wins per surface: a
	// second cassette claiming an already-held param is refused at admission.
	Param string `json:"param"`

	// Surface names the core endpoint family the param extends ("sessions").
	Surface string `json:"surface"`

	// View is the published view the filter probes; it must be declared in
	// publishes.views. The name reaches core's SQL as an identifier, which is
	// why its grammar is strict and core quotes it when rendering.
	View string `json:"view"`

	// Match tells core which rows of the view satisfy the claim.
	Match FilterMatch `json:"match"`

	// Normalize is the ordered list of verbs core applies to each supplied
	// value before binding it (vocabulary: trim, nfc, casefold). Order is
	// semantic — the verbs run in declared order — so canonicalization
	// preserves it rather than sorting.
	Normalize []string `json:"normalize,omitempty"`
}

// FilterMatch narrows the published view to the rows that satisfy a claim:
// rows whose primitive_type equals PrimitiveType and whose ValueColumn equals
// the normalized filter value, joined on the surface's own id.
type FilterMatch struct {
	PrimitiveType string `json:"primitive_type"`
	ValueColumn   string `json:"value_column"`
}

// Entity is one entity type a cassette offers for others to reference — the
// declaration side of the runtime entity registry. Core aggregates admitted
// declarations with its own core-native set into discovery; consumers learn
// the vocabulary from that surface rather than compiling in a list.
type Entity struct {
	// Type is the entity's stable type token ("skill").
	Type string `json:"type"`

	// IDKind names the shape of the entity's ids ("uuid", "string").
	IDKind string `json:"id_kind"`

	// DisplayName is the human-readable singular name.
	DisplayName string `json:"display_name,omitempty"`

	// Relations are declared links to other entity types. They are metadata
	// for future aggregation views; nothing consumes them yet.
	Relations []EntityRelation `json:"relations,omitempty"`
}

// EntityRelation declares a directed relation from the declaring entity type
// to another.
type EntityRelation struct {
	To   string `json:"to"`
	Kind string `json:"kind"`
}

// Hooks are paths on the cassette's own listener that core calls on events
// the cassette cares about. Delivery is best-effort: a hook that fails is
// logged and never affects admission, and polling bounds the staleness of
// anything a missed hook would have refreshed.
type Hooks struct {
	// RegistryChanged is POSTed whenever the admitted entity/claim set
	// changes, so a consumer of discovery can re-crawl immediately instead of
	// waiting out its polling interval.
	RegistryChanged string `json:"registry_changed,omitempty"`
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

// typeKey is hoisted because the JSON key recurs across the schema
// (settings, entities) and the linter insists on one spelling.
const typeKey = "type"

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
	if err := checkObjectKeys(root, "", "kind", "cassette", "depends", "api", "tables", "config",
		"publishes", "entities", "hooks", "x-source-digest"); err != nil {
		return err
	}
	if err := checkNestedObject(root["cassette"], "cassette",
		"name", "version", "display_name", "description", "license", "homepage", "image", "port", "audience"); err != nil {
		return err
	}
	if err := checkNestedObject(root["depends"], "depends", "core", "views", "published"); err != nil {
		return err
	}
	if err := checkNestedObject(root["api"], "api", "health", "openapi", "prefix_path"); err != nil {
		return err
	}
	if err := checkArrayObjects(root["tables"], "tables", []string{"name"}); err != nil {
		return err
	}
	if err := checkPublishesKeys(root["publishes"]); err != nil {
		return err
	}
	if err := checkEntitiesKeys(root["entities"]); err != nil {
		return err
	}
	if err := checkNestedObject(root["hooks"], "hooks", "registry_changed"); err != nil {
		return err
	}
	return checkArrayObjects(root["config"], "config", []string{
		"key", typeKey, "required", "default", "secret", "description", "enum", "min", "max",
	})
}

// checkPublishesKeys walks the publishes section, whose filters carry a nested
// match object that the flat checkArrayObjects cannot reach.
func checkPublishesKeys(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == jsonNull {
		return nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return fmt.Errorf("decode cassette metadata publishes object: %w", err)
	}
	if err := checkObjectKeys(object, "publishes", "views", "filters"); err != nil {
		return err
	}
	if len(object["filters"]) == 0 || string(object["filters"]) == jsonNull {
		return nil
	}
	var filters []json.RawMessage
	if err := json.Unmarshal(object["filters"], &filters); err != nil {
		return fmt.Errorf("decode cassette metadata publishes.filters array: %w", err)
	}
	for index, item := range filters {
		path := fmt.Sprintf("publishes.filters[%d]", index)
		var filter map[string]json.RawMessage
		if err := json.Unmarshal(item, &filter); err != nil {
			return fmt.Errorf("decode cassette metadata %s object: %w", path, err)
		}
		if err := checkObjectKeys(filter, path, "param", "surface", "view", "match", "normalize"); err != nil {
			return err
		}
		if err := checkNestedObject(filter["match"], path+".match", "primitive_type", "value_column"); err != nil {
			return err
		}
	}

	return nil
}

// checkEntitiesKeys walks the entities array, whose items carry a nested
// relations array that the flat checkArrayObjects cannot reach.
func checkEntitiesKeys(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == jsonNull {
		return nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return fmt.Errorf("decode cassette metadata entities array: %w", err)
	}
	for index, item := range items {
		path := fmt.Sprintf("entities[%d]", index)
		var entity map[string]json.RawMessage
		if err := json.Unmarshal(item, &entity); err != nil {
			return fmt.Errorf("decode cassette metadata %s object: %w", path, err)
		}
		if err := checkObjectKeys(entity, path, typeKey, "id_kind", "display_name", "relations"); err != nil {
			return err
		}
		if err := checkArrayObjects(entity["relations"], path+".relations", []string{"to", "kind"}); err != nil {
			return err
		}
	}

	return nil
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
	// The dynamic-registration sections are digest-relevant: a claim or entity
	// change is an identity change, because core and discovery consumers key
	// their behavior on what these declare. Absent stays absent, so a manifest
	// written before the sections existed keeps the digest it had.
	if m.Publishes != nil && (len(m.Publishes.Views) > 0 || len(m.Publishes.Filters) > 0) {
		root["publishes"] = canonicalPublishes(*m.Publishes)
	}
	if len(m.Entities) > 0 {
		root["entities"] = canonicalEntities(m.Entities)
	}
	if m.Hooks != nil && m.Hooks.RegistryChanged != "" {
		root["hooks"] = map[string]any{"registry_changed": m.Hooks.RegistryChanged}
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
	// Sorted, like every other set-like array here, so that declaring the same
	// audience in a different order is the same cassette by digest. Absent stays
	// absent: a manifest written before this field keeps the digest it had.
	if len(identity.Audience) > 0 {
		audience := append([]string(nil), identity.Audience...)
		sort.Strings(audience)
		value["audience"] = audience
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
	if len(depends.Published) > 0 {
		published := append([]string(nil), depends.Published...)
		sort.Strings(published)
		value["published"] = published
	}

	return value
}

func canonicalPublishes(publishes Publishes) map[string]any {
	value := map[string]any{}
	if len(publishes.Views) > 0 {
		views := append([]string(nil), publishes.Views...)
		sort.Strings(views)
		value["views"] = views
	}
	if len(publishes.Filters) > 0 {
		filters := append([]FilterClaim(nil), publishes.Filters...)
		sort.Slice(filters, func(i, j int) bool {
			if filters[i].Surface != filters[j].Surface {
				return filters[i].Surface < filters[j].Surface
			}

			return filters[i].Param < filters[j].Param
		})
		canonical := make([]map[string]any, 0, len(filters))
		for _, filter := range filters {
			entry := map[string]any{
				"param":   filter.Param,
				"surface": filter.Surface,
				"view":    filter.View,
				"match": map[string]any{
					"primitive_type": filter.Match.PrimitiveType,
					"value_column":   filter.Match.ValueColumn,
				},
			}
			// Normalize verbs run in declared order — sorting them would
			// change what core executes, so declaration order IS canonical.
			if len(filter.Normalize) > 0 {
				entry["normalize"] = append([]string(nil), filter.Normalize...)
			}
			canonical = append(canonical, entry)
		}
		value["filters"] = canonical
	}

	return value
}

func canonicalEntities(entities []Entity) []map[string]any {
	sorted := append([]Entity(nil), entities...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Type < sorted[j].Type })
	canonical := make([]map[string]any, 0, len(sorted))
	for _, entity := range sorted {
		entry := map[string]any{
			typeKey:   entity.Type,
			"id_kind": entity.IDKind,
		}
		putString(entry, "display_name", entity.DisplayName)
		if len(entity.Relations) > 0 {
			relations := append([]EntityRelation(nil), entity.Relations...)
			sort.Slice(relations, func(i, j int) bool {
				if relations[i].To != relations[j].To {
					return relations[i].To < relations[j].To
				}

				return relations[i].Kind < relations[j].Kind
			})
			entry["relations"] = relations
		}
		canonical = append(canonical, entry)
	}

	return canonical
}

func canonicalSetting(setting Setting) map[string]any {
	value := map[string]any{
		"key":   setting.Key,
		typeKey: setting.Type,
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
	redacted.Cassette.Audience = append([]string(nil), m.Cassette.Audience...)
	redacted.Depends.Views = append([]string(nil), m.Depends.Views...)
	redacted.Depends.Published = append([]string(nil), m.Depends.Published...)
	redacted.Tables = append([]Table(nil), m.Tables...)
	if m.Publishes != nil {
		publishes := Publishes{
			Views:   append([]string(nil), m.Publishes.Views...),
			Filters: append([]FilterClaim(nil), m.Publishes.Filters...),
		}
		for index := range publishes.Filters {
			publishes.Filters[index].Normalize = append([]string(nil), m.Publishes.Filters[index].Normalize...)
		}
		redacted.Publishes = &publishes
	}
	redacted.Entities = append([]Entity(nil), m.Entities...)
	for index := range redacted.Entities {
		redacted.Entities[index].Relations = append([]EntityRelation(nil), m.Entities[index].Relations...)
	}
	if m.Hooks != nil {
		hooks := *m.Hooks
		redacted.Hooks = &hooks
	}
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
