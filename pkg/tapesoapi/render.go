package tapesoapi

import (
	"fmt"
	"maps"
	"strings"
)

// render turns the merged IR into the generic tree that is marshalled to
// JSON or YAML.
//
// This is the *only* place the target version is consulted. Everything upstream
// — ingestion, merge, reference resolution — is version-neutral, which is what
// makes adding a version a new render function rather than a second pipeline.
func render(document *merged, target Version) (map[string]any, error) {
	r := renderer{target: target}
	out := map[string]any{
		"openapi": target.String(),
		"info":    r.info(document.info),
	}
	maps.Copy(out, document.extensions)
	if len(document.servers) > 0 {
		servers := make([]any, 0, len(document.servers))
		for _, server := range document.servers {
			servers = append(servers, r.server(server))
		}
		out["servers"] = servers
	}
	if len(document.tags) > 0 {
		tags := make([]any, 0, len(document.tags))
		for _, tag := range document.tags {
			tags = append(tags, r.tag(tag))
		}
		out["tags"] = tags
	}
	if len(document.security) > 0 {
		security := make([]any, 0, len(document.security))
		for _, requirement := range document.security {
			security = append(security, requirement)
		}
		out["security"] = security
	}

	paths := map[string]any{}
	for _, path := range sortedKeys(document.paths) {
		item, err := r.pathItem(document.paths[path])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		paths[path] = item
	}
	// 3.0 requires the paths object even when empty; 3.1 does not, but emitting
	// it there too keeps one shape for consumers that index into it blindly.
	out["paths"] = paths

	if target == V31 && len(document.webhooks) > 0 {
		webhooks := map[string]any{}
		for _, name := range sortedKeys(document.webhooks) {
			item, err := r.pathItem(document.webhooks[name])
			if err != nil {
				return nil, fmt.Errorf("webhook %s: %w", name, err)
			}
			webhooks[name] = item
		}
		out["webhooks"] = webhooks
	}

	components, err := r.components(document.components)
	if err != nil {
		return nil, err
	}
	if len(components) > 0 {
		out["components"] = components
	}

	return out, nil
}

type renderer struct {
	target Version
}

func (r renderer) info(info *Info) map[string]any {
	out := map[string]any{"title": info.Title, "version": info.Version}
	setString(out, "description", info.Description)
	setString(out, "termsOfService", info.TermsOfService)
	if info.Contact != nil {
		contact := map[string]any{}
		setString(contact, "name", info.Contact.Name)
		setString(contact, "url", info.Contact.URL)
		setString(contact, "email", info.Contact.Email)
		if len(contact) > 0 {
			out["contact"] = contact
		}
	}
	if info.License != nil {
		license := map[string]any{"name": info.License.Name}
		setString(license, "url", info.License.URL)
		// identifier is 3.1-only and is mutually exclusive with url there.
		if r.target == V31 && info.License.Identifier != "" {
			license["identifier"] = info.License.Identifier
			delete(license, "url")
		}
		out["license"] = license
	}
	maps.Copy(out, info.Extensions)

	return out
}

func (r renderer) server(server Server) map[string]any {
	out := map[string]any{"url": server.URL}
	setString(out, "description", server.Description)
	if len(server.Variables) > 0 {
		variables := map[string]any{}
		for name, variable := range server.Variables {
			entry := map[string]any{defaultKeyword: variable.Default}
			setString(entry, "description", variable.Description)
			if len(variable.Enum) > 0 {
				entry["enum"] = variable.Enum
			}
			variables[name] = entry
		}
		out["variables"] = variables
	}

	return out
}

func (r renderer) tag(tag Tag) map[string]any {
	out := map[string]any{"name": tag.Name}
	setString(out, "description", tag.Description)
	if tag.ExternalDocs != nil {
		out["externalDocs"] = r.externalDocs(tag.ExternalDocs)
	}

	return out
}

func (r renderer) externalDocs(docs *ExternalDocs) map[string]any {
	out := map[string]any{"url": docs.URL}
	setString(out, "description", docs.Description)

	return out
}

func (r renderer) pathItem(item *PathItem) (map[string]any, error) {
	out := map[string]any{}
	if item.Ref != "" {
		out["$ref"] = item.Ref

		return out, nil
	}
	setString(out, "summary", item.Summary)
	setString(out, "description", item.Description)
	if len(item.Parameters) > 0 {
		parameters, err := r.parameters(item.Parameters)
		if err != nil {
			return nil, err
		}
		out["parameters"] = parameters
	}
	if len(item.Servers) > 0 {
		servers := make([]any, 0, len(item.Servers))
		for _, server := range item.Servers {
			servers = append(servers, r.server(server))
		}
		out["servers"] = servers
	}
	maps.Copy(out, item.Extensions)
	for _, method := range item.Methods() {
		operation, err := r.operation(item.Operations[method])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", method, err)
		}
		out[strings.ToLower(method)] = operation
	}

	return out, nil
}

func (r renderer) operation(operation *Operation) (map[string]any, error) {
	out := map[string]any{}
	setString(out, "operationId", operation.OperationID)
	setString(out, "summary", operation.Summary)
	setString(out, "description", operation.Description)
	if len(operation.Tags) > 0 {
		out["tags"] = operation.Tags
	}
	if operation.Deprecated {
		out["deprecated"] = true
	}
	if len(operation.Parameters) > 0 {
		parameters, err := r.parameters(operation.Parameters)
		if err != nil {
			return nil, err
		}
		out["parameters"] = parameters
	}
	if operation.RequestBody != nil {
		body, err := r.requestBody(operation.RequestBody)
		if err != nil {
			return nil, err
		}
		out["requestBody"] = body
	}

	responses := map[string]any{}
	for _, status := range statusOrder(sortedKeys(operation.Responses)) {
		response, err := r.response(operation.Responses[status])
		if err != nil {
			return nil, fmt.Errorf("response %s: %w", status, err)
		}
		responses[status] = response
	}
	// Both versions require a responses object on every operation. An operation
	// that declares none gets a documented default rather than a validation
	// failure, because "undocumented outcome" is a gap the lint rules report,
	// not a reason to refuse to render.
	if len(responses) == 0 {
		responses[defaultResponseKey] = map[string]any{"description": "undocumented response"}
	}
	out["responses"] = responses

	if len(operation.Security) > 0 {
		security := make([]any, 0, len(operation.Security))
		for _, requirement := range operation.Security {
			security = append(security, requirement)
		}
		out["security"] = security
	}
	if len(operation.Servers) > 0 {
		servers := make([]any, 0, len(operation.Servers))
		for _, server := range operation.Servers {
			servers = append(servers, r.server(server))
		}
		out["servers"] = servers
	}
	if operation.ExternalDocs != nil {
		out["externalDocs"] = r.externalDocs(operation.ExternalDocs)
	}
	maps.Copy(out, operation.Extensions)

	return out, nil
}

func (r renderer) parameters(parameters []*Parameter) ([]any, error) {
	out := make([]any, 0, len(parameters))
	for _, parameter := range parameters {
		rendered, err := r.parameter(parameter)
		if err != nil {
			return nil, err
		}
		out = append(out, rendered)
	}

	return out, nil
}

func (r renderer) parameter(parameter *Parameter) (map[string]any, error) {
	if parameter.Ref != "" {
		return map[string]any{"$ref": parameter.Ref}, nil
	}
	out := map[string]any{"name": parameter.Name, "in": string(parameter.In)}
	setString(out, "description", parameter.Description)
	if parameter.Required {
		out["required"] = true
	}
	if parameter.Deprecated {
		out["deprecated"] = true
	}
	if parameter.Schema != nil {
		schema, err := r.schema(parameter.Schema)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", parameter.Name, err)
		}
		out["schema"] = schema
	}
	if parameter.Example != nil {
		out["example"] = parameter.Example
	}
	setString(out, "style", parameter.Style)
	if parameter.Explode != nil {
		out["explode"] = *parameter.Explode
	}
	maps.Copy(out, parameter.Extensions)

	return out, nil
}

func (r renderer) requestBody(body *RequestBody) (map[string]any, error) {
	if body.Ref != "" {
		return map[string]any{"$ref": body.Ref}, nil
	}
	out := map[string]any{}
	setString(out, "description", body.Description)
	if body.Required {
		out["required"] = true
	}
	content, err := r.content(body.Content)
	if err != nil {
		return nil, err
	}
	out["content"] = content
	maps.Copy(out, body.Extensions)

	return out, nil
}

func (r renderer) response(response *Response) (map[string]any, error) {
	if response.Ref != "" {
		return map[string]any{"$ref": response.Ref}, nil
	}
	description := response.Description
	if description == "" {
		// The field is required by both versions. Filling it in beats failing:
		// a missing description is a documentation gap the lint rules can
		// report, not a structural defect that should block a render.
		description = "response"
	}
	out := map[string]any{"description": description}
	if len(response.Content) > 0 {
		content, err := r.content(response.Content)
		if err != nil {
			return nil, err
		}
		out["content"] = content
	}
	if len(response.Headers) > 0 {
		headers := map[string]any{}
		for _, name := range sortedKeys(response.Headers) {
			header, err := r.header(response.Headers[name])
			if err != nil {
				return nil, fmt.Errorf("header %q: %w", name, err)
			}
			headers[name] = header
		}
		out["headers"] = headers
	}
	maps.Copy(out, response.Extensions)

	return out, nil
}

func (r renderer) header(header *Header) (map[string]any, error) {
	if header.Ref != "" {
		return map[string]any{"$ref": header.Ref}, nil
	}
	out := map[string]any{}
	setString(out, "description", header.Description)
	if header.Required {
		out["required"] = true
	}
	if header.Deprecated {
		out["deprecated"] = true
	}
	if header.Schema != nil {
		schema, err := r.schema(header.Schema)
		if err != nil {
			return nil, err
		}
		out["schema"] = schema
	}

	return out, nil
}

func (r renderer) content(content map[string]*MediaType) (map[string]any, error) {
	out := map[string]any{}
	for _, mediaType := range sortedKeys(content) {
		entry := content[mediaType]
		rendered := map[string]any{}
		if entry != nil {
			if entry.Schema != nil {
				schema, err := r.schema(entry.Schema)
				if err != nil {
					return nil, fmt.Errorf("content %q: %w", mediaType, err)
				}
				rendered["schema"] = schema
			}
			if entry.Example != nil {
				rendered["example"] = entry.Example
			}
			if len(entry.Examples) > 0 {
				rendered["examples"] = entry.Examples
			}
			if len(entry.Encoding) > 0 {
				encodings := map[string]any{}
				for name, encoding := range entry.Encoding {
					value := map[string]any{}
					setString(value, "contentType", encoding.ContentType)
					setString(value, "style", encoding.Style)
					if encoding.Explode != nil {
						value["explode"] = *encoding.Explode
					}
					encodings[name] = value
				}
				rendered["encoding"] = encodings
			}
			maps.Copy(rendered, entry.Extensions)
		}
		out[mediaType] = rendered
	}

	return out, nil
}

func (r renderer) components(components *Components) (map[string]any, error) {
	if components.IsEmpty() {
		return nil, nil
	}
	out := map[string]any{}

	if len(components.Schemas) > 0 {
		schemas := map[string]any{}
		for _, name := range sortedKeys(components.Schemas) {
			schema, err := r.schema(components.Schemas[name])
			if err != nil {
				return nil, fmt.Errorf("schemas/%s: %w", name, err)
			}
			schemas[name] = schema
		}
		out["schemas"] = schemas
	}
	if len(components.Responses) > 0 {
		responses := map[string]any{}
		for _, name := range sortedKeys(components.Responses) {
			response, err := r.response(components.Responses[name])
			if err != nil {
				return nil, fmt.Errorf("responses/%s: %w", name, err)
			}
			responses[name] = response
		}
		out["responses"] = responses
	}
	if len(components.Parameters) > 0 {
		parameters := map[string]any{}
		for _, name := range sortedKeys(components.Parameters) {
			parameter, err := r.parameter(components.Parameters[name])
			if err != nil {
				return nil, fmt.Errorf("parameters/%s: %w", name, err)
			}
			parameters[name] = parameter
		}
		out["parameters"] = parameters
	}
	if len(components.RequestBodies) > 0 {
		bodies := map[string]any{}
		for _, name := range sortedKeys(components.RequestBodies) {
			body, err := r.requestBody(components.RequestBodies[name])
			if err != nil {
				return nil, fmt.Errorf("requestBodies/%s: %w", name, err)
			}
			bodies[name] = body
		}
		out["requestBodies"] = bodies
	}
	if len(components.Headers) > 0 {
		headers := map[string]any{}
		for _, name := range sortedKeys(components.Headers) {
			header, err := r.header(components.Headers[name])
			if err != nil {
				return nil, fmt.Errorf("headers/%s: %w", name, err)
			}
			headers[name] = header
		}
		out["headers"] = headers
	}
	if len(components.Examples) > 0 {
		out["examples"] = components.Examples
	}
	if len(components.SecuritySchemes) > 0 {
		schemes := map[string]any{}
		for _, name := range sortedKeys(components.SecuritySchemes) {
			scheme := components.SecuritySchemes[name]
			value := map[string]any{"type": scheme.Type}
			setString(value, "description", scheme.Description)
			setString(value, "name", scheme.Name)
			setString(value, "in", scheme.In)
			setString(value, "scheme", scheme.Scheme)
			setString(value, "bearerFormat", scheme.BearerFormat)
			setString(value, "openIdConnectUrl", scheme.OpenIDConnectURL)
			if len(scheme.Flows) > 0 {
				value["flows"] = scheme.Flows
			}
			maps.Copy(value, scheme.Extensions)
			schemes[name] = value
		}
		out["securitySchemes"] = schemes
	}

	return out, nil
}

func setString(out map[string]any, key, value string) {
	if value != "" {
		out[key] = value
	}
}
