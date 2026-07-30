// Package gosource reads doc comments out of Go source so reflected schemas
// can carry prose.
//
// Reflection sees structure and nothing else: Go's runtime does not retain doc
// comments, so a purely reflective OpenAPI schema is structurally complete and
// entirely undocumented. The alternative the ecosystem settled on — encoding
// documentation into struct tags or magic comments — puts a second, unchecked
// language in the source. This package takes the third option: the doc comment
// stays an ordinary doc comment, and the generator reads it from the source it
// is already sitting in.
//
// Only the generator needs this. A shipped binary serves a contract that was
// compiled with the comments already folded in, so nothing here runs at
// runtime.
package gosource

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/mod/modfile"
)

// Docs is a lookup from Go type and field to doc comment.
//
// It implements tapesoapi.TypeDocs, which is the only thing the reflector needs
// from it.
type Docs struct {
	types map[string]*typeDoc
}

type typeDoc struct {
	doc    string
	fields map[string]string
}

// Option configures a Load.
type Option func(*options)

type options struct {
	skipDirs map[string]struct{}
}

// SkipDirs omits directories, by base name, from the scan.
func SkipDirs(names ...string) Option {
	return func(o *options) {
		if o.skipDirs == nil {
			o.skipDirs = map[string]struct{}{}
		}
		for _, name := range names {
			o.skipDirs[name] = struct{}{}
		}
	}
}

// Load scans a module rooted at dir and indexes its doc comments.
//
// The module path comes from go.mod, so the keys match the PkgPath reflection
// reports — which is what lets a reflected type find its own comments without
// the caller maintaining a mapping.
func Load(dir string, opts ...Option) (*Docs, error) {
	resolved := options{skipDirs: map[string]struct{}{
		"vendor": {}, "testdata": {}, "node_modules": {},
	}}
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}

	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	modulePath, err := modulePath(root)
	if err != nil {
		return nil, err
	}

	docs := &Docs{types: map[string]*typeDoc{}}
	fileSet := token.NewFileSet()

	err = filepath.WalkDir(root, func(current string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			if current == root {
				return nil
			}
			if _, skip := resolved.skipDirs[name]; skip {
				return filepath.SkipDir
			}
			// Directories Go itself ignores: dotfiles and underscore-prefixed.
			if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
				return filepath.SkipDir
			}
			// A nested module is a different module with different import
			// paths; indexing it under this module's path would attach the
			// wrong comments to a same-named type.
			if _, statErr := os.Stat(filepath.Join(current, "go.mod")); statErr == nil {
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}

		relative, err := filepath.Rel(root, filepath.Dir(current))
		if err != nil {
			return err
		}
		pkgPath := modulePath
		if relative != "." {
			pkgPath = path.Join(modulePath, filepath.ToSlash(relative))
		}

		file, err := parser.ParseFile(fileSet, current, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			// A file that does not parse is a compile error the build will
			// report far better than this scan can. Missing its comments is
			// not worth failing generation over.
			return nil //nolint:nilerr // an unparsable file is skipped, not fatal
		}
		docs.indexFile(pkgPath, file)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return docs, nil
}

func modulePath(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", fmt.Errorf("read go.mod for module path: %w", err)
	}
	parsed := modfile.ModulePath(data)
	if parsed == "" {
		return "", fmt.Errorf("go.mod at %s declares no module path", root)
	}

	return parsed, nil
}

func (d *Docs) indexFile(pkgPath string, file *ast.File) {
	for _, declaration := range file.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.TYPE {
			continue
		}
		for _, spec := range generic.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			// A single-spec declaration carries its comment on the GenDecl
			// (`// Foo is…` above `type Foo struct`), while a grouped one
			// carries it on the spec. Both spellings are the same comment to a
			// reader, so both are indexed.
			comment := commentText(typeSpec.Doc)
			if comment == "" && len(generic.Specs) == 1 {
				comment = commentText(generic.Doc)
			}

			entry := d.entry(pkgPath, typeSpec.Name.Name)
			if comment != "" {
				entry.doc = comment
			}

			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok || structType.Fields == nil {
				continue
			}
			for _, field := range structType.Fields.List {
				text := commentText(field.Doc)
				if text == "" {
					text = commentText(field.Comment)
				}
				if text == "" {
					continue
				}
				for _, name := range field.Names {
					entry.fields[name.Name] = text
				}
				if len(field.Names) == 0 {
					// An embedded field is named by its type.
					if name := embeddedName(field.Type); name != "" {
						entry.fields[name] = text
					}
				}
			}
		}
	}
}

func (d *Docs) entry(pkgPath, typeName string) *typeDoc {
	key := pkgPath + "." + typeName
	if existing, ok := d.types[key]; ok {
		return existing
	}
	entry := &typeDoc{fields: map[string]string{}}
	d.types[key] = entry

	return entry
}

func embeddedName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.StarExpr:
		return embeddedName(typed.X)
	case *ast.SelectorExpr:
		return typed.Sel.Name
	default:
		return ""
	}
}

// commentText renders a comment group the way a reader sees it: markers
// stripped, indentation removed, blank trailing lines dropped.
//
// Directive comments (`//go:generate`, `//nolint`) are excluded — they are
// instructions to tooling, not documentation, and publishing them in an API
// contract tells a client nothing.
func commentText(group *ast.CommentGroup) string {
	if group == nil {
		return ""
	}
	var lines []string
	for _, comment := range group.List {
		text := comment.Text
		switch {
		case strings.HasPrefix(text, "//"):
			text = strings.TrimPrefix(text, "//")
			if isDirective(text) {
				continue
			}
			lines = append(lines, strings.TrimPrefix(text, " "))
		case strings.HasPrefix(text, "/*"):
			text = strings.TrimSuffix(strings.TrimPrefix(text, "/*"), "*/")
			for line := range strings.SplitSeq(text, "\n") {
				lines = append(lines, strings.TrimSpace(line))
			}
		}
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}

	return strings.Join(lines, "\n")
}

// isDirective reports whether a comment body is a tooling directive, which Go
// defines as `name:value` with no space before the colon.
func isDirective(body string) bool {
	if strings.HasPrefix(body, " ") || body == "" {
		return false
	}
	colon := strings.IndexByte(body, ':')
	if colon <= 0 {
		return false
	}
	for _, r := range body[:colon] {
		if r != '_' && r != '-' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}

	return true
}

// TypeDoc implements tapesoapi.TypeDocs.
func (d *Docs) TypeDoc(pkgPath, typeName string) string {
	if d == nil {
		return ""
	}
	entry, ok := d.types[pkgPath+"."+typeName]
	if !ok {
		return ""
	}

	return entry.doc
}

// FieldDoc implements tapesoapi.TypeDocs.
func (d *Docs) FieldDoc(pkgPath, typeName, fieldName string) string {
	if d == nil {
		return ""
	}
	entry, ok := d.types[pkgPath+"."+typeName]
	if !ok {
		return ""
	}

	return entry.fields[fieldName]
}

// Len reports how many types were indexed, for a generator that wants to say so.
func (d *Docs) Len() int {
	if d == nil {
		return 0
	}

	return len(d.types)
}
