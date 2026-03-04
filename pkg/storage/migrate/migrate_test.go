package migrate_test

import (
	"context"
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/migrate"
)

var _ = Describe("Migrator", func() {
	var (
		db  *sql.DB
		ctx context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		db, err = sql.Open("sqlite3", ":memory:")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}
	})

	Describe("NewMigrator", func() {
		It("rejects unknown dialects", func() {
			_, err := migrate.NewMigrator(db, "mysql", nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unsupported dialect"))
		})

		It("accepts sqlite dialect", func() {
			m, err := migrate.NewMigrator(db, migrate.DialectSQLite, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(m).NotTo(BeNil())
		})

		It("accepts postgres dialect", func() {
			m, err := migrate.NewMigrator(db, migrate.DialectPostgres, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(m).NotTo(BeNil())
		})
	})

	Describe("Apply", func() {
		It("applies migrations in order", func() {
			migrations := []migrate.Migration{
				{
					Version:     1,
					Description: "create test table",
					Up:          "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);",
				},
				{
					Version:     2,
					Description: "add age column",
					Up:          "ALTER TABLE test ADD COLUMN age INTEGER;",
				},
			}

			m, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations)
			Expect(err).NotTo(HaveOccurred())
			Expect(m.Apply(ctx)).To(Succeed())

			// Verify both migrations were applied
			version, err := m.CurrentVersion(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(version).To(Equal(2))

			// Verify the table exists with both columns
			_, err = db.ExecContext(ctx, "INSERT INTO test (id, name, age) VALUES (1, 'alice', 30)")
			Expect(err).NotTo(HaveOccurred())
		})

		It("is idempotent", func() {
			migrations := []migrate.Migration{
				{
					Version:     1,
					Description: "create test table",
					Up:          "CREATE TABLE test (id INTEGER PRIMARY KEY);",
				},
			}

			m, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations)
			Expect(err).NotTo(HaveOccurred())

			Expect(m.Apply(ctx)).To(Succeed())
			Expect(m.Apply(ctx)).To(Succeed()) // second apply should be a no-op

			version, err := m.CurrentVersion(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(version).To(Equal(1))
		})

		It("skips already-applied migrations", func() {
			migrations1 := []migrate.Migration{
				{
					Version:     1,
					Description: "create test table",
					Up:          "CREATE TABLE test (id INTEGER PRIMARY KEY);",
				},
			}

			m1, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations1)
			Expect(err).NotTo(HaveOccurred())
			Expect(m1.Apply(ctx)).To(Succeed())

			// Add a second migration
			migrations2 := []migrate.Migration{
				{
					Version:     1,
					Description: "create test table",
					Up:          "CREATE TABLE test (id INTEGER PRIMARY KEY);",
				},
				{
					Version:     2,
					Description: "add name column",
					Up:          "ALTER TABLE test ADD COLUMN name TEXT;",
				},
			}

			m2, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations2)
			Expect(err).NotTo(HaveOccurred())
			Expect(m2.Apply(ctx)).To(Succeed())

			version, err := m2.CurrentVersion(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(version).To(Equal(2))
		})

		It("returns error for migration with empty SQL", func() {
			migrations := []migrate.Migration{
				{
					Version:     1,
					Description: "empty migration",
					Up:          "",
				},
			}

			m, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations)
			Expect(err).NotTo(HaveOccurred())

			err = m.Apply(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("has no SQL"))
		})

		It("rolls back failed migrations", func() {
			migrations := []migrate.Migration{
				{
					Version:     1,
					Description: "create test table",
					Up:          "CREATE TABLE test (id INTEGER PRIMARY KEY);",
				},
				{
					Version:     2,
					Description: "bad migration",
					Up:          "INVALID SQL SYNTAX HERE;",
				},
			}

			m, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations)
			Expect(err).NotTo(HaveOccurred())

			err = m.Apply(ctx)
			Expect(err).To(HaveOccurred())

			// First migration should have been applied
			version, err := m.CurrentVersion(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(version).To(Equal(1))
		})

		It("enforces unique version constraint", func() {
			migrations := []migrate.Migration{
				{
					Version:     1,
					Description: "create test table",
					Up:          "CREATE TABLE test (id INTEGER PRIMARY KEY);",
				},
			}

			m, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations)
			Expect(err).NotTo(HaveOccurred())
			Expect(m.Apply(ctx)).To(Succeed())

			// Manually insert a duplicate version row to simulate corruption
			_, err = db.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES (1)")
			Expect(err).To(HaveOccurred()) // UNIQUE constraint violation
		})
	})

	Describe("Gap detection", func() {
		It("detects corrupted migration state with gaps", func() {
			migrations := []migrate.Migration{
				{
					Version:     1,
					Description: "create test table",
					Up:          "CREATE TABLE test (id INTEGER PRIMARY KEY);",
				},
			}

			m, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations)
			Expect(err).NotTo(HaveOccurred())
			Expect(m.Apply(ctx)).To(Succeed())

			// Manually insert a version 3 row, creating a gap (no version 2)
			_, err = db.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES (3)")
			Expect(err).NotTo(HaveOccurred())

			// Now try to apply again — should detect the gap
			migrations2 := []migrate.Migration{
				{Version: 1, Description: "create test table", Up: "CREATE TABLE test (id INTEGER PRIMARY KEY);"},
				{Version: 2, Description: "add column", Up: "ALTER TABLE test ADD COLUMN name TEXT;"},
				{Version: 3, Description: "add another column", Up: "ALTER TABLE test ADD COLUMN age INTEGER;"},
				{Version: 4, Description: "add email column", Up: "ALTER TABLE test ADD COLUMN email TEXT;"},
			}

			m2, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations2)
			Expect(err).NotTo(HaveOccurred())

			err = m2.Apply(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("migration state corrupt"))
		})
	})

	Describe("CurrentVersion", func() {
		It("returns 0 for a fresh database", func() {
			m, err := migrate.NewMigrator(db, migrate.DialectSQLite, nil)
			Expect(err).NotTo(HaveOccurred())

			version, err := m.CurrentVersion(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(version).To(Equal(0))
		})
	})

	Describe("Sorts migrations by version", func() {
		It("applies out-of-order migrations correctly", func() {
			// Provide migrations in reverse order
			migrations := []migrate.Migration{
				{
					Version:     2,
					Description: "add name column",
					Up:          "ALTER TABLE test ADD COLUMN name TEXT;",
				},
				{
					Version:     1,
					Description: "create test table",
					Up:          "CREATE TABLE test (id INTEGER PRIMARY KEY);",
				},
			}

			m, err := migrate.NewMigrator(db, migrate.DialectSQLite, migrations)
			Expect(err).NotTo(HaveOccurred())
			Expect(m.Apply(ctx)).To(Succeed())

			version, err := m.CurrentVersion(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(version).To(Equal(2))
		})
	})
})
