package migrate

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// MigrationsFromFS reads all .sql files from an [fs.FS] (typically produced
// by //go:embed) and returns an ordered slice of [Migration] values.
//
// File names must follow the pattern "NNN_description.sql" where NNN is a
// zero-padded integer version (e.g. "001_baseline_schema.sql"). Files that
// don't match this pattern are silently skipped.
func MigrationsFromFS(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("reading migrations directory: %w", err)
	}

	var migrations []Migration
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".sql" {
			continue
		}

		version, description, ok := parseMigrationFilename(e.Name())
		if !ok {
			continue
		}

		data, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("reading migration file %s: %w", e.Name(), err)
		}

		migrations = append(migrations, Migration{
			Version:     version,
			Description: description,
			Up:          string(data),
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

// parseMigrationFilename extracts the version number and description from
// a filename like "001_baseline_schema.sql". Returns false if the name
// doesn't match the expected pattern.
func parseMigrationFilename(name string) (int, string, bool) {
	base := strings.TrimSuffix(name, filepath.Ext(name)) // "001_baseline_schema"
	parts := strings.SplitN(base, "_", 2)
	if len(parts) < 2 {
		return 0, "", false
	}

	version, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", false
	}

	description := strings.ReplaceAll(parts[1], "_", " ")
	return version, description, true
}
