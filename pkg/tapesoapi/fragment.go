package tapesoapi

import (
	"context"
	"fmt"
	"sort"
)

// Kind names where a fragment came from. It is a string rather than an enum so
// a caller implementing Source can name its own kind without patching this
// package.
const (
	KindDocument = "document"
	KindRoute    = "route"
	KindManual   = "manual"
	KindReflect  = "reflect"
)

// Provenance records where a contribution came from, precisely enough that a
// conflict error can point a reader at the two places to go look.
//
// This is load-bearing rather than logging: aggregation only stays usable at
// scale if every error names its sources. "GET /users/{id} is defined twice" is
// a puzzle; "defined by both specs/users.yaml and route users.go:41" is a fix.
type Provenance struct {
	// Kind is the class of contributor: KindDocument, KindRoute, KindManual, or
	// a caller-defined kind.
	Kind string

	// Name identifies the contributor within its kind — a file path, a URL, or
	// a route pattern such as "GET /users/:id".
	Name string

	// Detail is optional extra location information, typically a file:line.
	Detail string
}

// String renders a provenance for an error message.
func (p Provenance) String() string {
	switch {
	case p.Kind == "" && p.Name == "":
		return "<unknown source>"
	case p.Detail != "":
		return fmt.Sprintf("%s %s (%s)", p.Kind, p.Name, p.Detail)
	case p.Kind == "":
		return p.Name
	default:
		return p.Kind + " " + p.Name
	}
}

// sortKey orders fragments deterministically at merge time. Merging in
// provenance order rather than insertion order is what makes compilation
// reproducible when routes register concurrently.
func (p Provenance) sortKey() string { return p.Kind + "\x00" + p.Name + "\x00" + p.Detail }

// Fragment is a partial OpenAPI contribution plus where it came from.
//
// An external YAML file, a Fiber route registration, and a hand-built operation
// are all the same thing to the parser. Collapsing them into one type is what
// lets ingestion and live route registration share a single merge, validate,
// and render pipeline instead of two that drift.
type Fragment struct {
	Provenance Provenance

	// Version is the version the fragment was authored against, empty for
	// fragments built programmatically (which are version-neutral by
	// construction). It is what lets a compile refuse to silently downgrade a
	// 3.1 document.
	Version Version

	Info       *Info
	Servers    []Server
	Tags       []Tag
	Security   []SecurityRequirement
	Paths      map[string]*PathItem
	Components *Components

	// Webhooks are accepted from 3.1 documents and rendered only for 3.1
	// targets. Holding them in the IR now means adding 3.1 output later does
	// not change this type.
	Webhooks map[string]*PathItem

	// Authoritative marks this fragment's Info as the one that wins. Without
	// it, two fragments both setting Info is a conflict rather than a
	// last-one-loaded race.
	Authoritative bool

	Extensions map[string]any
}

// Source is anything that can contribute fragments.
//
// Document ingestion and the Fiber adapter are both just implementations, and
// a caller can add its own — pulling specs from a service registry, say —
// without this package shipping support for it.
type Source interface {
	Fragments(ctx context.Context) ([]Fragment, error)
}

// SourceFunc adapts a function to Source.
type SourceFunc func(ctx context.Context) ([]Fragment, error)

// Fragments implements Source.
func (f SourceFunc) Fragments(ctx context.Context) ([]Fragment, error) { return f(ctx) }

// paths returns this fragment's path keys in stable order.
func (f Fragment) paths() []string {
	keys := make([]string, 0, len(f.Paths))
	for path := range f.Paths {
		keys = append(keys, path)
	}
	sort.Strings(keys)

	return keys
}

// clone deep-copies a fragment so the parser owns its state outright. Without
// it, a caller mutating a schema it also handed to AddFragment would change a
// document already compiled from it.
func (f Fragment) clone() Fragment {
	out := f
	out.Servers = append([]Server(nil), f.Servers...)
	out.Tags = append([]Tag(nil), f.Tags...)
	out.Security = cloneSecurity(f.Security)
	out.Extensions = cloneAnyMap(f.Extensions)
	if f.Info != nil {
		info := *f.Info
		info.Extensions = cloneAnyMap(f.Info.Extensions)
		out.Info = &info
	}
	out.Paths = clonePathItems(f.Paths)
	out.Webhooks = clonePathItems(f.Webhooks)
	out.Components = f.Components.clone()

	return out
}

func cloneSecurity(in []SecurityRequirement) []SecurityRequirement {
	if in == nil {
		return nil
	}
	out := make([]SecurityRequirement, len(in))
	for i, requirement := range in {
		copied := make(SecurityRequirement, len(requirement))
		for scheme, scopes := range requirement {
			copied[scheme] = append([]string(nil), scopes...)
		}
		out[i] = copied
	}

	return out
}

func clonePathItems(in map[string]*PathItem) map[string]*PathItem {
	if in == nil {
		return nil
	}
	out := make(map[string]*PathItem, len(in))
	for path, item := range in {
		out[path] = item.clone()
	}

	return out
}

func (p *PathItem) clone() *PathItem {
	if p == nil {
		return nil
	}
	out := *p
	out.Servers = append([]Server(nil), p.Servers...)
	out.Extensions = cloneAnyMap(p.Extensions)
	out.Parameters = cloneParameters(p.Parameters)
	if p.Operations != nil {
		out.Operations = make(map[string]*Operation, len(p.Operations))
		for method, operation := range p.Operations {
			out.Operations[method] = operation.clone()
		}
	}

	return &out
}

func (o *Operation) clone() *Operation {
	if o == nil {
		return nil
	}
	out := *o
	out.Tags = append([]string(nil), o.Tags...)
	out.Servers = append([]Server(nil), o.Servers...)
	out.Security = cloneSecurity(o.Security)
	out.Extensions = cloneAnyMap(o.Extensions)
	out.Parameters = cloneParameters(o.Parameters)
	out.RequestBody = o.RequestBody.clone()
	if o.Responses != nil {
		out.Responses = make(map[string]*Response, len(o.Responses))
		for status, response := range o.Responses {
			out.Responses[status] = response.clone()
		}
	}

	return &out
}

func cloneParameters(in []*Parameter) []*Parameter {
	if in == nil {
		return nil
	}
	out := make([]*Parameter, len(in))
	for i, parameter := range in {
		out[i] = parameter.clone()
	}

	return out
}

func (p *Parameter) clone() *Parameter {
	if p == nil {
		return nil
	}
	out := *p
	out.Schema = p.Schema.clone()
	out.Extensions = cloneAnyMap(p.Extensions)

	return &out
}

func (r *RequestBody) clone() *RequestBody {
	if r == nil {
		return nil
	}
	out := *r
	out.Content = cloneContent(r.Content)
	out.Extensions = cloneAnyMap(r.Extensions)

	return &out
}

func (r *Response) clone() *Response {
	if r == nil {
		return nil
	}
	out := *r
	out.Content = cloneContent(r.Content)
	out.Extensions = cloneAnyMap(r.Extensions)
	if r.Headers != nil {
		out.Headers = make(map[string]*Header, len(r.Headers))
		for name, header := range r.Headers {
			out.Headers[name] = header.clone()
		}
	}

	return &out
}

func (h *Header) clone() *Header {
	if h == nil {
		return nil
	}
	out := *h
	out.Schema = h.Schema.clone()

	return &out
}

func cloneContent(in map[string]*MediaType) map[string]*MediaType {
	if in == nil {
		return nil
	}
	out := make(map[string]*MediaType, len(in))
	for mediaType, entry := range in {
		if entry == nil {
			out[mediaType] = nil

			continue
		}
		copied := *entry
		copied.Schema = entry.Schema.clone()
		copied.Extensions = cloneAnyMap(entry.Extensions)
		copied.Examples = cloneAnyMap(entry.Examples)
		out[mediaType] = &copied
	}

	return out
}

func (c *Components) clone() *Components {
	if c == nil {
		return nil
	}
	out := &Components{}
	if c.Schemas != nil {
		out.Schemas = make(map[string]*Schema, len(c.Schemas))
		for name, schema := range c.Schemas {
			out.Schemas[name] = schema.clone()
		}
	}
	if c.Responses != nil {
		out.Responses = make(map[string]*Response, len(c.Responses))
		for name, response := range c.Responses {
			out.Responses[name] = response.clone()
		}
	}
	if c.Parameters != nil {
		out.Parameters = make(map[string]*Parameter, len(c.Parameters))
		for name, parameter := range c.Parameters {
			out.Parameters[name] = parameter.clone()
		}
	}
	if c.RequestBodies != nil {
		out.RequestBodies = make(map[string]*RequestBody, len(c.RequestBodies))
		for name, body := range c.RequestBodies {
			out.RequestBodies[name] = body.clone()
		}
	}
	if c.Headers != nil {
		out.Headers = make(map[string]*Header, len(c.Headers))
		for name, header := range c.Headers {
			out.Headers[name] = header.clone()
		}
	}
	out.Examples = cloneAnyMap(c.Examples)
	if c.SecuritySchemes != nil {
		out.SecuritySchemes = make(map[string]*SecurityScheme, len(c.SecuritySchemes))
		for name, scheme := range c.SecuritySchemes {
			copied := *scheme
			copied.Flows = cloneAnyMap(scheme.Flows)
			copied.Extensions = cloneAnyMap(scheme.Extensions)
			out.SecuritySchemes[name] = &copied
		}
	}

	return out
}
