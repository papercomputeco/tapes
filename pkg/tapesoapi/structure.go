package tapesoapi

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// This file is the structural validator: the rules the OpenAPI specification
// states as requirements, checked over the merged IR just before it renders.
//
// It validates the IR rather than the rendered bytes, and it is this package's
// own code rather than a second library's opinion of it. Both are deliberate.
//
// Round-tripping the output through a third-party implementation sounds like
// the stronger check, and for a renderer nobody owns it would be. Here it buys
// less than it costs. Every byte this package emits comes from a closed,
// typed IR through one renderer, so the failures a second parser would catch
// are failures of *rendering*, which the render tests pin directly and
// exhaustively. What it cannot catch is the failure that actually happens: an
// ingested document, or a route description, that says something the spec
// forbids. That needs the provenance — which file, which registration site —
// and provenance is exactly what survives to the IR and is gone from the bytes.
//
// The cost was a dependency on a pre-v1 library in the path of every compile,
// whose types and error shapes move between releases and whose opinions we
// would inherit without being able to explain them. A published contract
// should not be able to fail to build because someone else's validator changed
// its mind.
//
// A rule belongs here when the specification says MUST and a client generator
// would break without it. A rule that is merely good practice belongs in a
// LintRule, where it can be switched off.

// StructureError reports every structural violation at once, so one compile
// names the whole list rather than the first item on it.
type StructureError struct {
	// Version is the target the document was checked against, because some
	// rules only apply to one of them.
	Version Version

	// Violations are the findings, sorted and deduplicated.
	Violations []string
}

func (e *StructureError) Error() string {
	if len(e.Violations) == 1 {
		return fmt.Sprintf("openapi %s: %s", e.Version, e.Violations[0])
	}

	return fmt.Sprintf("%d OpenAPI %s structure violations:\n  - %s",
		len(e.Violations), e.Version, strings.Join(e.Violations, "\n  - "))
}

// componentName is the character set OpenAPI permits for a component key.
// Namespacing an ingested document prepends to these, so a name that is not
// expressible here would produce a document no reader can resolve refs in.
var componentName = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// securitySchemeTypes are the scheme types and the fields each requires. A
// scheme missing its own required field is unusable by a client, which is worse
// than absent: the client generates an auth path that cannot work.
var securitySchemeTypes = map[string][]string{
	"apiKey":        {"name", "in"},
	"http":          {"scheme"},
	"oauth2":        {"flows"},
	"openIdConnect": {"openIdConnectUrl"},
	"mutualTLS":     nil,
}

// structure accumulates findings so one pass reports everything.
type structure struct {
	target Version

	// defined is held rather than passed down because the only rule that
	// needs it — a security requirement naming a defined scheme — is checked at
	// the operation level, several frames below where it is known.
	defined *Components

	violations []string
}

func (s *structure) reportf(format string, args ...any) {
	s.violations = append(s.violations, fmt.Sprintf(format, args...))
}

// validateStructure checks a merged document against the specification for the
// version it is about to be rendered to.
func validateStructure(document *merged, target Version) error {
	check := &structure{target: target, defined: document.components}

	check.info(document.info)
	check.servers(document.servers)
	for index, tag := range document.tags {
		if strings.TrimSpace(tag.Name) == "" {
			check.reportf("tags[%d] has no name", index)
		}
	}
	check.securityRequirements("root security", document.security)
	check.paths(document.paths, "path")
	check.webhooks(document.webhooks)
	check.components(document.components)

	if len(check.violations) == 0 {
		return nil
	}
	sort.Strings(check.violations)

	return &StructureError{Version: target, Violations: dedupeStrings(check.violations)}
}

func (s *structure) info(info *Info) {
	// Both are unconditionally required, and both are defaulted before this
	// runs — so a finding here means the default itself was defeated, which is
	// worth saying rather than silently repairing twice.
	if info == nil {
		s.reportf("info is required")

		return
	}
	if strings.TrimSpace(info.Title) == "" {
		s.reportf("info.title is required")
	}
	if strings.TrimSpace(info.Version) == "" {
		s.reportf("info.version is required")
	}
	if info.License != nil && strings.TrimSpace(info.License.Name) == "" {
		s.reportf("info.license.name is required when a license is given")
	}
}

func (s *structure) servers(servers []Server) {
	for index, server := range servers {
		if strings.TrimSpace(server.URL) == "" {
			s.reportf("servers[%d] has no url", index)

			continue
		}
		// A server URL is a template, and an undeclared variable in one leaves
		// a client with a literal `{host}` in its base URL.
		for _, name := range PathParams(server.URL) {
			variable, declared := server.Variables[name]
			if !declared {
				s.reportf("servers[%d] url %q uses {%s}, which no server variable declares",
					index, server.URL, name)

				continue
			}
			if variable == nil || variable.Default == "" {
				s.reportf("servers[%d] variable %q has no default, which the spec requires",
					index, name)
			}
		}
	}
}

// paths checks every path and the operations on it. kind names the section, so
// the same rules can report against webhooks without pretending they are paths.
func (s *structure) paths(paths map[string]*PathItem, kind string) {
	for _, path := range sortedKeys(paths) {
		item := paths[path]
		if item == nil {
			continue
		}
		if kind == "path" && !strings.HasPrefix(path, "/") {
			s.reportf("path %q must begin with /", path)
		}

		declared := PathParams(path)
		seen := map[string]bool{}
		for _, name := range declared {
			if seen[name] {
				s.reportf("path %q declares {%s} more than once", path, name)
			}
			seen[name] = true
		}

		s.parameters(path, item.Parameters, nil)
		for _, method := range item.Methods() {
			s.operation(path, method, item.Operations[method], item.Parameters, declared)
		}
	}
}

// webhooks are keyed by name rather than by path, and have no template
// parameters to reconcile — so they get the operation rules and none of the
// path ones.
//
// Against a 3.0 target there is nothing to check: the render has nowhere to put
// them and drops them, and reaching a 3.0 target with webhooks present already
// required WithDowngradeLossy, which is where that trade is stated.
func (s *structure) webhooks(webhooks map[string]*PathItem) {
	if len(webhooks) == 0 || s.target == V30 {
		return
	}
	for _, name := range sortedKeys(webhooks) {
		item := webhooks[name]
		if item == nil {
			continue
		}
		for _, method := range item.Methods() {
			s.operation("webhook "+name, method, item.Operations[method], item.Parameters, nil)
		}
	}
}

func (s *structure) operation(
	path, method string,
	operation *Operation,
	shared []*Parameter,
	templateParams []string,
) {
	if operation == nil {
		return
	}
	where := method + " " + path

	s.parameters(where, operation.Parameters, shared)
	s.securityRequirements(where, operation.Security)

	// Every template parameter must be described, and every described path
	// parameter must be in the template. Both directions matter: the first
	// leaves a generated client with an unnamed argument, the second with an
	// argument that goes nowhere.
	described := map[string]bool{}
	opaque := false
	for _, parameter := range append(append([]*Parameter(nil), shared...), operation.Parameters...) {
		resolved := s.resolveParameter(parameter)
		if resolved == nil {
			// A parameter this package cannot see through could be the very one
			// the template needs, so neither direction can be judged. Both
			// checks stand down for this operation rather than guess.
			opaque = opaque || parameter != nil
			continue
		}
		if resolved.In != InPath {
			continue
		}
		described[resolved.Name] = true
		if !resolved.Required {
			s.reportf("%s path parameter %q must be required", where, resolved.Name)
		}
	}
	if !opaque {
		inTemplate := map[string]bool{}
		for _, name := range templateParams {
			inTemplate[name] = true
			if !described[name] {
				s.reportf("%s does not describe path parameter {%s} (from %s)",
					where, name, operation.provenance)
			}
		}
		for _, name := range sortedStrings(described) {
			if !inTemplate[name] {
				s.reportf("%s describes path parameter %q, which %q does not contain (from %s)",
					where, name, path, operation.provenance)
			}
		}
	}

	if body := operation.RequestBody; body != nil && body.Ref == "" {
		if len(body.Content) == 0 {
			s.reportf("%s has a request body with no content", where)
		}
		s.content(where+" request body", body.Content)
	}

	for _, status := range sortedKeys(operation.Responses) {
		response := operation.Responses[status]
		if response == nil || response.Ref != "" {
			continue
		}
		s.content(fmt.Sprintf("%s response %s", where, status), response.Content)
		for _, name := range sortedKeys(response.Headers) {
			if header := response.Headers[name]; header != nil && header.Ref == "" {
				s.schema(fmt.Sprintf("%s response %s header %q", where, status, name), header.Schema)
			}
		}
	}
}

// resolveParameter follows a local component reference, so a parameter written
// once and shared by reference is checked like an inline one — the shape the
// fixtures and every hand-written contract actually use.
//
// nil means "cannot see through this": an external reference, or one that does
// not resolve. A dangling local reference is resolveReferences' finding to
// report, and inventing a second, worse-worded one here would double it.
func (s *structure) resolveParameter(parameter *Parameter) *Parameter {
	for hops := 0; parameter != nil && parameter.Ref != ""; hops++ {
		name, local := strings.CutPrefix(parameter.Ref, componentsParameterPrefix)
		if !local || s.defined == nil || hops > 8 {
			return nil
		}
		parameter = s.defined.Parameters[name]
	}

	return parameter
}

// parameters checks one parameter list, plus its uniqueness against the
// path-level list it inherits.
func (s *structure) parameters(where string, parameters, inherited []*Parameter) {
	seen := map[string]bool{}
	for _, parameter := range inherited {
		if resolved := s.resolveParameter(parameter); resolved != nil {
			seen[string(resolved.In)+" "+resolved.Name] = true
		}
	}
	for index, parameter := range parameters {
		if parameter == nil {
			s.reportf("%s parameters[%d] is nil", where, index)

			continue
		}
		if parameter.Ref != "" {
			// The component itself is checked once, where it is defined. What is
			// still this list's business is that referencing it twice — or
			// alongside an inline twin — does not describe one parameter twice.
			resolved := s.resolveParameter(parameter)
			if resolved == nil {
				continue
			}
			key := string(resolved.In) + " " + resolved.Name
			if seen[key] {
				s.reportf("%s declares parameter %q in %s more than once",
					where, resolved.Name, resolved.In)
			}
			seen[key] = true

			continue
		}
		if strings.TrimSpace(parameter.Name) == "" {
			s.reportf("%s parameters[%d] has no name", where, index)
		}
		switch parameter.In {
		case InPath, InQuery, InHeader, InCookie:
		case "":
			s.reportf("%s parameter %q has no `in`", where, parameter.Name)
		default:
			s.reportf("%s parameter %q has in=%q, which is not one of path, query, header, cookie",
				where, parameter.Name, parameter.In)
		}
		key := string(parameter.In) + " " + parameter.Name
		if seen[key] {
			s.reportf("%s declares parameter %q in %s more than once",
				where, parameter.Name, parameter.In)
		}
		seen[key] = true

		s.schema(fmt.Sprintf("%s parameter %q", where, parameter.Name), parameter.Schema)
	}
}

func (s *structure) content(where string, content map[string]*MediaType) {
	for _, mediaType := range sortedKeys(content) {
		// A media type key is a media range, and a client dispatches on it. A
		// key that is not one means the body is unreachable by content
		// negotiation.
		if !strings.Contains(mediaType, "/") {
			s.reportf("%s declares content type %q, which is not a media type", where, mediaType)
		}
		entry := content[mediaType]
		if entry == nil {
			continue
		}
		s.schema(fmt.Sprintf("%s content %q", where, mediaType), entry.Schema)
	}
}

func (s *structure) components(components *Components) {
	if components == nil {
		return
	}
	checkNames := func(section string, names []string) {
		for _, name := range names {
			if !componentName.MatchString(name) {
				s.reportf("components/%s key %q must match %s", section, name, componentName)
			}
		}
	}
	checkNames("schemas", sortedKeys(components.Schemas))
	checkNames("responses", sortedKeys(components.Responses))
	checkNames("parameters", sortedKeys(components.Parameters))
	checkNames("requestBodies", sortedKeys(components.RequestBodies))
	checkNames("headers", sortedKeys(components.Headers))
	checkNames("examples", sortedKeys(components.Examples))
	checkNames("securitySchemes", sortedKeys(components.SecuritySchemes))

	for _, name := range sortedKeys(components.Schemas) {
		s.schema("schemas/"+name, components.Schemas[name])
	}
	for _, name := range sortedKeys(components.Responses) {
		if response := components.Responses[name]; response != nil {
			s.content("responses/"+name, response.Content)
		}
	}
	for _, name := range sortedKeys(components.Parameters) {
		s.parameters("parameters/"+name, []*Parameter{components.Parameters[name]}, nil)
	}
	for _, name := range sortedKeys(components.RequestBodies) {
		body := components.RequestBodies[name]
		if body == nil || body.Ref != "" {
			continue
		}
		if len(body.Content) == 0 {
			s.reportf("requestBodies/%s has no content", name)
		}
		s.content("requestBodies/"+name, body.Content)
	}
	for _, name := range sortedKeys(components.SecuritySchemes) {
		s.securityScheme(name, components.SecuritySchemes[name])
	}
}

func (s *structure) securityScheme(name string, scheme *SecurityScheme) {
	if scheme == nil {
		s.reportf("securitySchemes/%s is nil", name)

		return
	}
	required, known := securitySchemeTypes[scheme.Type]
	if !known {
		s.reportf("securitySchemes/%s has type %q, which is not one of %s",
			name, scheme.Type, strings.Join(sortedKeys(securitySchemeTypes), ", "))

		return
	}
	if scheme.Type == "mutualTLS" && s.target == V30 {
		s.reportf("securitySchemes/%s uses mutualTLS, which is 3.1-only", name)
	}
	present := map[string]bool{
		"name":             scheme.Name != "",
		"in":               scheme.In != "",
		"scheme":           scheme.Scheme != "",
		"flows":            len(scheme.Flows) > 0,
		"openIdConnectUrl": scheme.OpenIDConnectURL != "",
	}
	for _, field := range required {
		if !present[field] {
			s.reportf("securitySchemes/%s is type %q and requires %s", name, scheme.Type, field)
		}
	}
	if scheme.Type == "apiKey" && scheme.In != "" {
		switch scheme.In {
		case "query", "header", "cookie":
		default:
			s.reportf("securitySchemes/%s has in=%q, which apiKey does not allow", name, scheme.In)
		}
	}
}

// securityRequirements checks that a requirement names a scheme the document
// defines. An undefined one silently disables auth on the operation it guards.
func (s *structure) securityRequirements(where string, requirements []SecurityRequirement) {
	for _, requirement := range requirements {
		for _, name := range sortedKeys(requirement) {
			if s.defined == nil || s.defined.SecuritySchemes[name] == nil {
				s.reportf("%s requires security scheme %q, which components/securitySchemes does not define",
					where, name)
			}
		}
	}
}

// schema checks one schema and everything under it.
//
// A `$ref` is not followed: reference targets are checked once by
// resolveReferences, and following them here would validate a shared component
// once per referrer and report it that many times.
func (s *structure) schema(where string, schema *Schema) {
	if schema == nil || schema.Ref != "" {
		return
	}

	switch schema.Type {
	case "", TypeString, TypeNumber, TypeInteger, TypeBoolean, TypeArray, TypeObject, TypeNull:
	default:
		s.reportf("%s has type %q, which is not a JSON Schema type", where, schema.Type)
	}

	s.bounds(where, schema)

	if schema.Pattern != "" {
		if _, err := regexp.Compile(schema.Pattern); err != nil {
			// Go's regexp is RE2 and the spec says ECMA-262, so this rejects a
			// little more than it must — backreferences and lookarounds. It is
			// still worth checking: a pattern this package cannot compile is
			// one it cannot validate an instance against either.
			s.reportf("%s has pattern %q, which does not compile: %v", where, schema.Pattern, err)
		}
	}

	if schema.Discriminator != nil {
		if strings.TrimSpace(schema.Discriminator.PropertyName) == "" {
			s.reportf("%s has a discriminator with no propertyName", where)
		}
		if len(schema.OneOf)+len(schema.AnyOf)+len(schema.AllOf) == 0 {
			s.reportf("%s has a discriminator but no oneOf, anyOf, or allOf to select from", where)
		}
	}

	s.schema(where+".items", schema.Items)
	s.schema(where+".not", schema.Not)
	s.schema(where+".additionalProperties", schema.AdditionalProperties)
	for _, name := range sortedKeys(schema.Properties) {
		s.schema(where+".properties."+name, schema.Properties[name])
	}
	for index, member := range schema.OneOf {
		s.schema(fmt.Sprintf("%s.oneOf[%d]", where, index), member)
	}
	for index, member := range schema.AnyOf {
		s.schema(fmt.Sprintf("%s.anyOf[%d]", where, index), member)
	}
	for index, member := range schema.AllOf {
		s.schema(fmt.Sprintf("%s.allOf[%d]", where, index), member)
	}
}

// bounds checks the constraint keywords for coherence.
//
// An incoherent pair — minLength above maxLength — is not a spec violation, it
// is a schema no instance can satisfy. That is worth failing a build over,
// because nothing downstream will ever report it: the generated client compiles
// and every payload is rejected at runtime.
func (s *structure) bounds(where string, schema *Schema) {
	if schema.MultipleOf != nil && *schema.MultipleOf <= 0 {
		s.reportf("%s has multipleOf %g, which must be greater than zero", where, *schema.MultipleOf)
	}
	if schema.Minimum != nil && schema.Maximum != nil && *schema.Minimum > *schema.Maximum {
		s.reportf("%s has minimum %g above maximum %g", where, *schema.Minimum, *schema.Maximum)
	}
	if schema.MinLength != nil && schema.MaxLength != nil && *schema.MinLength > *schema.MaxLength {
		s.reportf("%s has minLength %d above maxLength %d", where, *schema.MinLength, *schema.MaxLength)
	}
	if schema.MinItems != nil && schema.MaxItems != nil && *schema.MinItems > *schema.MaxItems {
		s.reportf("%s has minItems %d above maxItems %d", where, *schema.MinItems, *schema.MaxItems)
	}
	if schema.MinProperties != nil && schema.MaxProperties != nil && *schema.MinProperties > *schema.MaxProperties {
		s.reportf("%s has minProperties %d above maxProperties %d",
			where, *schema.MinProperties, *schema.MaxProperties)
	}
}

// sortedStrings returns the true keys of a set, sorted, so findings come out in
// a stable order.
func sortedStrings(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key, ok := range set {
		if ok {
			out = append(out, key)
		}
	}
	sort.Strings(out)

	return out
}
