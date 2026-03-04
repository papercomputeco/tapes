package migrate_test

import (
	"io/fs"
	"testing/fstest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/migrate"
)

var _ = Describe("MigrationsFromFS", func() {
	It("parses valid migration files", func() {
		fsys := fstest.MapFS{
			"001_create_table.sql": &fstest.MapFile{Data: []byte("CREATE TABLE test (id INT);")},
			"002_add_column.sql":   &fstest.MapFile{Data: []byte("ALTER TABLE test ADD COLUMN name TEXT;")},
		}

		migrations, err := migrate.MigrationsFromFS(fsys)
		Expect(err).NotTo(HaveOccurred())
		Expect(migrations).To(HaveLen(2))

		Expect(migrations[0].Version).To(Equal(1))
		Expect(migrations[0].Description).To(Equal("create table"))
		Expect(migrations[0].Up).To(Equal("CREATE TABLE test (id INT);"))

		Expect(migrations[1].Version).To(Equal(2))
		Expect(migrations[1].Description).To(Equal("add column"))
		Expect(migrations[1].Up).To(Equal("ALTER TABLE test ADD COLUMN name TEXT;"))
	})

	It("skips non-SQL files", func() {
		fsys := fstest.MapFS{
			"001_create_table.sql": &fstest.MapFile{Data: []byte("CREATE TABLE test (id INT);")},
			"README.md":            &fstest.MapFile{Data: []byte("This is a readme")},
			"notes.txt":            &fstest.MapFile{Data: []byte("Some notes")},
		}

		migrations, err := migrate.MigrationsFromFS(fsys)
		Expect(err).NotTo(HaveOccurred())
		Expect(migrations).To(HaveLen(1))
	})

	It("skips files with invalid naming", func() {
		fsys := fstest.MapFS{
			"001_create_table.sql": &fstest.MapFile{Data: []byte("CREATE TABLE test (id INT);")},
			"bad_name.sql":         &fstest.MapFile{Data: []byte("SELECT 1;")},
			"no_underscore.sql":    &fstest.MapFile{Data: []byte("SELECT 1;")},
		}

		migrations, err := migrate.MigrationsFromFS(fsys)
		Expect(err).NotTo(HaveOccurred())
		Expect(migrations).To(HaveLen(1))
	})

	It("returns empty slice for empty FS", func() {
		fsys := fstest.MapFS{}

		migrations, err := migrate.MigrationsFromFS(fsys)
		Expect(err).NotTo(HaveOccurred())
		Expect(migrations).To(BeEmpty())
	})

	It("sorts migrations by version", func() {
		fsys := fstest.MapFS{
			"003_third.sql":  &fstest.MapFile{Data: []byte("SELECT 3;")},
			"001_first.sql":  &fstest.MapFile{Data: []byte("SELECT 1;")},
			"002_second.sql": &fstest.MapFile{Data: []byte("SELECT 2;")},
		}

		migrations, err := migrate.MigrationsFromFS(fsys)
		Expect(err).NotTo(HaveOccurred())
		Expect(migrations).To(HaveLen(3))
		Expect(migrations[0].Version).To(Equal(1))
		Expect(migrations[1].Version).To(Equal(2))
		Expect(migrations[2].Version).To(Equal(3))
	})

	It("skips directories", func() {
		fsys := fstest.MapFS{
			"001_create_table.sql": &fstest.MapFile{Data: []byte("CREATE TABLE test (id INT);")},
			"subdir":               &fstest.MapFile{Mode: fs.ModeDir | 0o755},
		}

		migrations, err := migrate.MigrationsFromFS(fsys)
		Expect(err).NotTo(HaveOccurred())
		Expect(migrations).To(HaveLen(1))
	})
})
