package tapesoapi

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// This file holds the lint rules. The specification's own requirements are
// checked in structure.go; the difference is that a violation there means the
// document is invalid, while a finding here means it is valid and still a bad
// idea. Lint is therefore replaceable per compile ([WithLint]) and structure is
// not.

// LintRule is one check over a merged document. Rules run alongside structural
// validation, so they can assume the document is well formed and concern
// themselves with whether it is *good*.
type LintRule interface {
	// Name identifies the rule in error output.
	Name() string

	// Check returns one finding per problem, empty when the document passes.
	Check(document *LintTarget) []string
}

// LintTarget is the read-only view of a merged document that lint rules see.
// It is a distinct type from the internal merge state so adding a rule never
// requires reaching into compile internals.
type LintTarget struct {
	Info       *Info
	Paths      map[string]*PathItem
	Components *Components
}

// LintError reports every lint finding at once.
type LintError struct {
	Findings []string
}

func (e *LintError) Error() string {
	if len(e.Findings) == 1 {
		return "openapi lint: " + e.Findings[0]
	}

	return fmt.Sprintf("%d openapi lint findings:\n  - %s",
		len(e.Findings), strings.Join(e.Findings, "\n  - "))
}

// DefaultLintRules are the rules a compile runs unless [WithLint] replaces
// them.
//
// They encode what the published tapes contracts actually need: every operation
// carries a unique operationId (progenitor panics without one, and a duplicate
// silently collapses two client methods into one), and every operation
// documents at least one outcome.
func DefaultLintRules() []LintRule {
	return []LintRule{
		OperationIDPresent{},
		OperationIDUnique{},
		ResponsesDeclared{},
		NoOrphanComponents{},
	}
}

func runLint(document *merged, rules []LintRule) error {
	if len(rules) == 0 {
		return nil
	}
	target := &LintTarget{
		Info:       document.info,
		Paths:      document.paths,
		Components: document.components,
	}
	var findings []string
	for _, rule := range rules {
		for _, finding := range rule.Check(target) {
			findings = append(findings, rule.Name()+": "+finding)
		}
	}
	if len(findings) == 0 {
		return nil
	}
	sort.Strings(findings)

	return &LintError{Findings: findings}
}

// OperationIDPresent requires an operationId on every operation.
type OperationIDPresent struct{}

// Name implements LintRule.
func (OperationIDPresent) Name() string { return "operation-id-present" }

// Check implements LintRule.
func (OperationIDPresent) Check(document *LintTarget) []string {
	var findings []string
	eachOperation(document, func(method, path string, operation *Operation) {
		if strings.TrimSpace(operation.OperationID) == "" {
			findings = append(findings, fmt.Sprintf("%s %s has no operationId (from %s)",
				method, path, operation.provenance))
		}
	})

	return findings
}

// OperationIDUnique requires operationIds to be distinct.
type OperationIDUnique struct{}

// Name implements LintRule.
func (OperationIDUnique) Name() string { return "operation-id-unique" }

// Check implements LintRule.
func (OperationIDUnique) Check(document *LintTarget) []string {
	owners := map[string][]string{}
	eachOperation(document, func(method, path string, operation *Operation) {
		if operation.OperationID == "" {
			return
		}
		owners[operation.OperationID] = append(owners[operation.OperationID],
			fmt.Sprintf("%s %s (from %s)", method, path, operation.provenance))
	})

	var findings []string
	for _, id := range sortedKeys(owners) {
		if len(owners[id]) < 2 {
			continue
		}
		findings = append(findings, fmt.Sprintf("operationId %q is used by %s",
			id, strings.Join(owners[id], " and ")))
	}

	return findings
}

// ResponsesDeclared requires at least one documented outcome per operation.
type ResponsesDeclared struct{}

// Name implements LintRule.
func (ResponsesDeclared) Name() string { return "responses-declared" }

// Check implements LintRule.
func (ResponsesDeclared) Check(document *LintTarget) []string {
	var findings []string
	eachOperation(document, func(method, path string, operation *Operation) {
		if len(operation.Responses) == 0 {
			findings = append(findings, fmt.Sprintf("%s %s documents no responses (from %s)",
				method, path, operation.provenance))

			return
		}
		for _, status := range sortedKeys(operation.Responses) {
			if status == defaultResponseKey || strings.HasSuffix(status, "XX") {
				continue
			}
			if code, err := strconv.Atoi(status); err != nil || code < 100 || code > 599 {
				findings = append(findings, fmt.Sprintf("%s %s declares response key %q, which is not a status code",
					method, path, status))
			}
		}
	})

	return findings
}

// NoOrphanComponents reports component schemas nothing references.
//
// An orphan is usually the residue of a deleted operation, and left in place it
// makes a generated client carry a type no endpoint produces.
type NoOrphanComponents struct{}

// Name implements LintRule.
func (NoOrphanComponents) Name() string { return "no-orphan-components" }

// Check implements LintRule.
func (NoOrphanComponents) Check(document *LintTarget) []string {
	if document.Components == nil || len(document.Components.Schemas) == 0 {
		return nil
	}

	// Reachability, not a single pass: a schema referenced only by another
	// reachable schema is not an orphan, and a one-pass reference count would
	// call it one.
	referenced := map[string]struct{}{}
	var visit func(ref string) string
	visit = func(ref string) string {
		name, ok := strings.CutPrefix(ref, componentsSchemaPrefix)
		if !ok {
			return ref
		}
		if _, seen := referenced[name]; seen {
			return ref
		}
		referenced[name] = struct{}{}
		if schema, defined := document.Components.Schemas[name]; defined {
			schema.walkRefs(visit)
		}

		return ref
	}

	eachOperation(document, func(_, _ string, operation *Operation) {
		walkOperationRefs(operation, visit)
	})
	for _, name := range sortedKeys(document.Components.Responses) {
		walkResponseRefs(document.Components.Responses[name], visit)
	}
	for _, name := range sortedKeys(document.Components.Parameters) {
		walkParameterRefs(document.Components.Parameters[name], visit)
	}
	for _, name := range sortedKeys(document.Components.RequestBodies) {
		walkRequestBodyRefs(document.Components.RequestBodies[name], visit)
	}

	var findings []string
	for _, name := range sortedKeys(document.Components.Schemas) {
		if _, used := referenced[name]; !used {
			findings = append(findings, fmt.Sprintf("schemas/%s is defined but never referenced", name))
		}
	}

	return findings
}

func eachOperation(document *LintTarget, visit func(method, path string, operation *Operation)) {
	for _, path := range sortedKeys(document.Paths) {
		item := document.Paths[path]
		for _, method := range item.Methods() {
			visit(method, path, item.Operations[method])
		}
	}
}
