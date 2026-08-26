package v1alpha1

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

var (
	contractVersionPattern = regexp.MustCompile(`^v[1-9][0-9]*$`)
	identifierPattern      = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
	settingKeyPattern      = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`)
	prefixSegmentPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
	digestPattern          = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	audiencePattern        = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

	// qualifiedViewPattern is the grammar for a published view name. It is
	// deliberately strict — schema-qualified, lowercase snake, at most 63
	// bytes per segment — because this identifier crosses the trust boundary:
	// a manifest is remote input, and the name eventually reaches core's SQL
	// in an identifier position (quoted, but validated here first; CC-5).
	qualifiedViewPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$`)
)

// knownFilterSurfaces are the core surfaces a filter claim may extend. The
// vocabulary is closed on purpose: a claim on a surface this build does not
// serve could never execute, and refusing it at validation is the operator-
// visible admission problem the mechanism promises.
var knownFilterSurfaces = []string{"sessions"}

// knownNormalizeVerbs is the value-normalization vocabulary core implements.
// A claim declaring a verb outside it is refused: core applies exactly the
// declared verbs, so an unknown one is a filter core cannot execute.
var knownNormalizeVerbs = []string{"trim", "nfc", "casefold"}

// Validate checks the manifest's intrinsic constraints and verifies that its
// declared tapes contract is in supported.
func (m *Manifest) Validate(supported []cassette.ContractVersion) error {
	if m == nil {
		return &cassette.ValidationError{Problems: []cassette.Problem{{Field: "manifest", Message: "must not be nil"}}}
	}

	problems := make([]cassette.Problem, 0)
	add := func(field, message string) {
		problems = append(problems, cassette.Problem{Field: field, Message: message})
	}

	if m.Kind != Kind {
		add("kind", fmt.Sprintf("must be %q", Kind))
	}
	if _, err := cassette.ParseName(string(m.Cassette.Name)); err != nil {
		add("cassette.name", err.Error())
	}
	if strings.TrimSpace(string(m.Cassette.Version)) == "" {
		add("cassette.version", "must not be empty")
	}
	if m.Cassette.Homepage != "" {
		parsed, err := url.Parse(m.Cassette.Homepage)
		if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			add("cassette.homepage", "must be an absolute http or https URL")
		}
	}
	if m.SourceDigest != "" && !digestPattern.MatchString(string(m.SourceDigest)) {
		add("x-source-digest", "must be sha256 followed by 64 lowercase hexadecimal characters")
	}

	if !contractVersionPattern.MatchString(string(m.Depends.Core)) {
		add("depends.core", "must match v<positive integer>")
	} else if !slices.Contains(supported, m.Depends.Core) {
		add("depends.core", fmt.Sprintf("contract version %q is not supported", m.Depends.Core))
	}

	seenViews := make(map[string]int, len(m.Depends.Views))
	for index, view := range m.Depends.Views {
		field := fmt.Sprintf("depends.views[%d]", index)
		if !identifierPattern.MatchString(view) {
			add(field, "must be a lower-snake Postgres identifier of at most 63 bytes")
		}
		if view == "raw_turns" {
			add(field, "raw_turns is not a contract view and may never be granted")
		}
		if previous, exists := seenViews[view]; exists {
			add(field, fmt.Sprintf("duplicates depends.views[%d]", previous))
		} else {
			seenViews[view] = index
		}
	}

	seenPublished := make(map[string]int, len(m.Depends.Published))
	for index, view := range m.Depends.Published {
		field := fmt.Sprintf("depends.published[%d]", index)
		if !qualifiedViewPattern.MatchString(view) {
			add(field, "must be a schema-qualified lower-snake view name (schema.view, each segment at most 63 bytes)")
		} else if reason := reservedPublishedSchema(view); reason != "" {
			add(field, reason)
		}
		if previous, exists := seenPublished[view]; exists {
			add(field, fmt.Sprintf("duplicates depends.published[%d]", previous))
		} else {
			seenPublished[view] = index
		}
	}

	seenAudience := make(map[string]int, len(m.Cassette.Audience))
	for index, client := range m.Cassette.Audience {
		field := fmt.Sprintf("cassette.audience[%d]", index)
		if !audiencePattern.MatchString(client) {
			add(field, "must be a lowercase client name of at most 63 bytes")
		}
		if previous, exists := seenAudience[client]; exists {
			add(field, fmt.Sprintf("duplicates cassette.audience[%d]", previous))
		} else {
			seenAudience[client] = index
		}
	}

	validateAPIPath("api.health", m.API.Health, add)
	validateAPIPath("api.openapi", m.API.OpenAPI, add)
	validatePrefixPath(m.API.Prefix, add)
	validatePackaging(m.Cassette, add)
	validatePublishes(m.Publishes, add)
	validateEntities(m.Entities, add)
	if m.Hooks != nil && m.Hooks.RegistryChanged != "" {
		validateAPIPath("hooks.registry_changed", m.Hooks.RegistryChanged, add)
	}

	seenTables := make(map[string]int, len(m.Tables))
	for index, table := range m.Tables {
		field := fmt.Sprintf("table[%d].name", index)
		if !identifierPattern.MatchString(table.Name) {
			add(field, "must be a lower-snake Postgres identifier of at most 63 bytes")
		}
		if previous, exists := seenTables[table.Name]; exists {
			add(field, fmt.Sprintf("duplicates table[%d].name", previous))
		} else {
			seenTables[table.Name] = index
		}
	}

	seenSettings := make(map[string]int, len(m.Config))
	seenEnvironment := make(map[string]int, len(m.Config))
	for index := range m.Config {
		setting := &m.Config[index]
		prefix := fmt.Sprintf("config[%d]", index)
		if !settingKeyPattern.MatchString(setting.Key) {
			add(prefix+".key", "must contain dotted lower-snake segments")
		}
		if previous, exists := seenSettings[setting.Key]; exists {
			add(prefix+".key", fmt.Sprintf("duplicates config[%d].key", previous))
		} else {
			seenSettings[setting.Key] = index
		}
		environmentKey := setting.EnvVar()
		if previous, exists := seenEnvironment[environmentKey]; exists {
			add(prefix+".key", fmt.Sprintf("projects to the same environment variable as config[%d].key", previous))
		} else {
			seenEnvironment[environmentKey] = index
		}
		validateSetting(setting, prefix, add)
	}

	if len(problems) > 0 {
		return &cassette.ValidationError{Problems: problems}
	}

	return nil
}

func validateAPIPath(field, value string, add func(string, string)) {
	if value == "" {
		add(field, "must not be empty")
		return
	}
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Path != value || !strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") {
		add(field, "must be an absolute path without a host, query, or fragment")
		return
	}
	for segment := range strings.SplitSeq(parsed.Path, "/") {
		if segment == "." || segment == ".." {
			add(field, "must not contain dot path segments")
			return
		}
	}
}

// validatePrefixPath checks the head core swaps for /v1/cassettes.
//
// The rule is narrow on purpose. This value is spliced into a path prefix that
// decides which process a request reaches, so a segment carrying a slash, a
// dot, or an empty string could make the cassette's declared prefix and the
// prefix core actually matches disagree — and that disagreement is a routing
// bug that only shows up in production.
func validatePrefixPath(value string, add func(string, string)) {
	const field = "api.prefix_path"

	if trimmed := strings.Trim(value, "/"); trimmed != "" {
		for segment := range strings.SplitSeq(trimmed, "/") {
			if !prefixSegmentPattern.MatchString(segment) {
				add(field, "must be slash-separated segments of lowercase letters, digits, dashes, and underscores")

				return
			}
		}

		return
	}

	// Everything that trims to nothing is "no prefix", but only "/" says so
	// deliberately — an empty string is what an author writes by accident, and
	// ApplyDefaults has already turned a genuinely absent field into "api".
	if value != "/" {
		add(field, `must not be empty; omit the field for the "api" default, or set "/" for no prefix`)
	}
}

// validatePackaging keeps the optional image metadata internally complete.
// Core does not run the image, but deployment tooling must not have to guess
// which port the packaged program serves.
func validatePackaging(identity Identity, add func(string, string)) {
	if identity.Image != "" && strings.TrimSpace(identity.Image) != identity.Image {
		add("cassette.image", "must not have leading or trailing whitespace")
	}
	if identity.Image != "" && identity.Port == 0 {
		add("cassette.port", "is required when cassette.image is set")
	}
	if identity.Port != 0 && (identity.Port < 1 || identity.Port > 65535) {
		add("cassette.port", "must be between 1 and 65535")
	}
	if identity.Port != 0 && identity.Image == "" {
		add("cassette.image", "is required when cassette.port is set")
	}
}

// reservedPublishedSchema reports why a published view's schema segment is off
// limits, or the empty string when it is fine. The reservations mirror the
// cassette-name rules: a published view lives in the same database core does,
// so the collisions worth refusing are the ones Postgres or core has already
// taken.
func reservedPublishedSchema(view string) string {
	schema, _, _ := strings.Cut(view, ".")
	switch {
	case schema == "public":
		return "must not use the public schema"
	case schema == "tapes" || strings.HasPrefix(schema, "tapes_"):
		return "must not use core's reserved tapes schemas"
	case strings.HasPrefix(schema, "pg_"):
		return "must not use the Postgres-reserved pg_ prefix"
	}

	return ""
}

// validatePublishes checks the published-view declarations and filter claims.
// Grammar only — collision with core-owned params and cross-cassette claims
// is admission-time state the runner owns, not an intrinsic manifest fact.
func validatePublishes(publishes *Publishes, add func(string, string)) {
	if publishes == nil {
		return
	}

	declared := make(map[string]int, len(publishes.Views))
	for index, view := range publishes.Views {
		field := fmt.Sprintf("publishes.views[%d]", index)
		if !qualifiedViewPattern.MatchString(view) {
			add(field, "must be a schema-qualified lower-snake view name (schema.view, each segment at most 63 bytes)")

			continue
		}
		if reason := reservedPublishedSchema(view); reason != "" {
			add(field, reason)

			continue
		}
		if previous, exists := declared[view]; exists {
			add(field, fmt.Sprintf("duplicates publishes.views[%d]", previous))
		} else {
			declared[view] = index
		}
	}

	seenClaims := make(map[string]int, len(publishes.Filters))
	for index := range publishes.Filters {
		filter := &publishes.Filters[index]
		prefix := fmt.Sprintf("publishes.filters[%d]", index)
		if !identifierPattern.MatchString(filter.Param) {
			add(prefix+".param", "must be a lower-snake query param name of at most 63 bytes")
		}
		if !slices.Contains(knownFilterSurfaces, filter.Surface) {
			add(prefix+".surface", "must be one of "+strings.Join(knownFilterSurfaces, ", "))
		}
		if _, ok := declared[filter.View]; !ok {
			add(prefix+".view", "must name a view declared in publishes.views")
		}
		if !identifierPattern.MatchString(filter.Match.PrimitiveType) {
			add(prefix+".match.primitive_type", "must be a lower-snake entity type of at most 63 bytes")
		}
		if !identifierPattern.MatchString(filter.Match.ValueColumn) {
			add(prefix+".match.value_column", "must be a lower-snake column name of at most 63 bytes")
		}
		for verbIndex, verb := range filter.Normalize {
			if !slices.Contains(knownNormalizeVerbs, verb) {
				add(fmt.Sprintf("%s.normalize[%d]", prefix, verbIndex),
					"must be one of "+strings.Join(knownNormalizeVerbs, ", "))
			}
		}
		key := filter.Surface + "\x00" + filter.Param
		if previous, exists := seenClaims[key]; exists {
			add(prefix+".param", fmt.Sprintf("duplicates publishes.filters[%d]", previous))
		} else {
			seenClaims[key] = index
		}
	}
}

// validateEntities checks the advertised entity declarations. The id_kind
// vocabulary is deliberately open (shape-checked, not membership-checked),
// mirroring the audience field: a cassette must be able to describe an id
// shape this build has not heard of.
func validateEntities(entities []Entity, add func(string, string)) {
	seen := make(map[string]int, len(entities))
	for index := range entities {
		entity := &entities[index]
		prefix := fmt.Sprintf("entities[%d]", index)
		if !identifierPattern.MatchString(entity.Type) {
			add(prefix+".type", "must be a lower-snake entity type of at most 63 bytes")
		}
		if !identifierPattern.MatchString(entity.IDKind) {
			add(prefix+".id_kind", "must be a lower-snake id kind such as uuid or string")
		}
		if previous, exists := seen[entity.Type]; exists {
			add(prefix+".type", fmt.Sprintf("duplicates entities[%d].type", previous))
		} else {
			seen[entity.Type] = index
		}
		for relationIndex, relation := range entity.Relations {
			relationPrefix := fmt.Sprintf("%s.relations[%d]", prefix, relationIndex)
			if !identifierPattern.MatchString(relation.To) {
				add(relationPrefix+".to", "must be a lower-snake entity type of at most 63 bytes")
			}
			if !identifierPattern.MatchString(relation.Kind) {
				add(relationPrefix+".kind", "must be a lower-snake relation kind of at most 63 bytes")
			}
		}
	}
}

func validateSetting(setting *Setting, prefix string, add func(string, string)) {
	if !setting.Type.valid() {
		add(prefix+".type", "must be one of "+strings.Join(settingTypeNames, ", "))
	}
	if setting.Secret && setting.Default != nil {
		add(prefix+".default", "secret settings must not declare a default")
	}

	if len(setting.Enum) > 0 && setting.Type != SettingTypeString {
		add(prefix+".enum", "is only valid for string settings")
	}
	seenEnum := make(map[string]int, len(setting.Enum))
	for index, enumValue := range setting.Enum {
		if previous, exists := seenEnum[enumValue]; exists {
			add(fmt.Sprintf("%s.enum[%d]", prefix, index), fmt.Sprintf("duplicates %s.enum[%d]", prefix, previous))
		} else {
			seenEnum[enumValue] = index
		}
	}

	if setting.Min != nil && setting.Type != SettingTypeInt {
		add(prefix+".min", "is only valid for int settings")
	}
	if setting.Max != nil && setting.Type != SettingTypeInt {
		add(prefix+".max", "is only valid for int settings")
	}
	if setting.Min != nil && setting.Max != nil && *setting.Min > *setting.Max {
		add(prefix+".min", "must be less than or equal to max")
	}

	if setting.Default == nil || !setting.Type.valid() {
		return
	}
	if message := validateTypedValue(setting, setting.Default); message != "" {
		add(prefix+".default", message)
	}
}

func validateTypedValue(setting *Setting, value any) string {
	switch setting.Type {
	case SettingTypeString:
		stringValue, ok := value.(string)
		if !ok {
			return "must be a string"
		}

		if len(setting.Enum) > 0 && !slices.Contains(setting.Enum, stringValue) {
			return "must be one of the declared enum values"
		}
	case SettingTypeInt:
		integer, ok := integerValue(value)
		if !ok {
			return "must be an integer"
		}
		if setting.Min != nil && integer < *setting.Min {
			return fmt.Sprintf("must be at least %d", *setting.Min)
		}
		if setting.Max != nil && integer > *setting.Max {
			return fmt.Sprintf("must be at most %d", *setting.Max)
		}
	case SettingTypeBool:
		if _, ok := value.(bool); !ok {
			return "must be a boolean"
		}
	case SettingTypeDuration:
		duration, ok := value.(string)
		if !ok {
			return "must be a duration string"
		}
		if _, err := time.ParseDuration(duration); err != nil {
			return "must be a valid Go duration"
		}
	case SettingTypeJSON:
		jsonValue, ok := value.(string)
		if !ok {
			return "must be a JSON string"
		}
		if !json.Valid([]byte(jsonValue)) {
			return "must contain valid JSON"
		}
	}

	return ""
}

func integerValue(value any) (int64, bool) {
	switch integer := value.(type) {
	case int:
		return int64(integer), true
	case int8:
		return int64(integer), true
	case int16:
		return int64(integer), true
	case int32:
		return int64(integer), true
	case int64:
		return integer, true
	case uint:
		if uint64(integer) > math.MaxInt64 {
			return 0, false
		}
		return int64(integer), true // #nosec G115 -- bounded to math.MaxInt64 above
	case uint8:
		return int64(integer), true
	case uint16:
		return int64(integer), true
	case uint32:
		return int64(integer), true
	case uint64:
		if integer > math.MaxInt64 {
			return 0, false
		}
		return int64(integer), true
	case float64:
		// float64(math.MaxInt64) rounds up to 2^63, so the upper bound must
		// be exclusive to avoid a conversion that wraps to math.MinInt64.
		if math.IsNaN(integer) || math.IsInf(integer, 0) || integer != math.Trunc(integer) || integer < math.MinInt64 || integer >= float64(math.MaxInt64) {
			return 0, false
		}
		return int64(integer), true
	case json.Number:
		parsed, err := strconv.ParseInt(string(integer), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
