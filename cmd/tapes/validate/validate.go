// Package validatecmder provides the hidden tapes validate command. It walks
// the parent edges of a store and reports cycles or dangling parent refs —
// the failure mode that turns pkg/storage/ent/driver.(*EntDriver).Ancestry
// into an infinite loop (cycle) or a silently truncated chain (dangling).
package validatecmder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
	"github.com/papercomputeco/tapes/pkg/validate"
)

// previewMaxChars bounds the `content_preview` field in JSON output so a
// single runaway message can't blow up the report size.
const previewMaxChars = 200

type validateCommander struct {
	flags config.FlagSet

	sqlitePath  string
	postgresDSN string
	format      string
	outputPath  string
	debug       bool

	logger *slog.Logger
}

var validateFlags = config.FlagSet{
	config.FlagSQLite:   {Name: "sqlite", Shorthand: "s", ViperKey: "storage.sqlite_path", Description: "Path to SQLite database"},
	config.FlagPostgres: {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string"},
}

// NewValidateCmd creates the hidden "tapes validate" command.
func NewValidateCmd() *cobra.Command {
	cmder := &validateCommander{flags: validateFlags}

	cmd := &cobra.Command{
		Use:           "validate [db-path]",
		Short:         "Check store DAG integrity (cycles, dangling parents)",
		Long:          "Walks every parent_hash edge and reports cycles or dangling parent references — the conditions that can make Ancestry() loop forever or silently truncate a chain at a missing parent.",
		Hidden:        true,
		Args:          cobra.MaximumNArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}
			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagSQLite,
				config.FlagPostgres,
			})
			cmder.sqlitePath = v.GetString("storage.sqlite_path")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			// Positional argument wins: `tapes validate ~/paper/brain.sqlite`
			// is the shortest path to pointing the check at a specific file.
			if len(args) == 1 {
				cmder.sqlitePath = args[0]
				cmder.postgresDSN = ""
			}

			switch cmder.format {
			case "", "text", "json":
			default:
				return fmt.Errorf("unknown --format %q: expected text or json", cmder.format)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}
			return cmder.run(cmd.Context())
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagSQLite, &cmder.sqlitePath)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	cmd.Flags().StringVar(&cmder.format, "format", "text", "Output format: text or json")
	cmd.Flags().StringVarP(&cmder.outputPath, "output", "o", "", "Write report to this path instead of stdout")

	return cmd
}

func (c *validateCommander) run(ctx context.Context) error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	// Refuse to validate a SQLite path that doesn't already exist — the
	// driver would otherwise happily create an empty database and report
	// a clean scan over zero rows, which is worse than useless.
	if c.postgresDSN == "" {
		if c.sqlitePath == "" {
			return errors.New("no storage configured: pass a db path, --sqlite, or --postgres")
		}
		if _, err := os.Stat(c.sqlitePath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("sqlite database does not exist: %s", c.sqlitePath)
			}
			return fmt.Errorf("stat sqlite database: %w", err)
		}
	}

	driver, err := c.newStorageDriver(ctx)
	if err != nil {
		return err
	}
	defer driver.Close()

	lister, ok := driver.(storage.ParentRefLister)
	if !ok {
		return errors.New("storage driver does not support integrity checks (missing ParentRefLister)")
	}

	report, err := validate.Check(ctx, lister)
	if err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	// Enrich the dangling entries with node metadata so triage doesn't
	// require a second pass through sqlite3. Cheap — there are typically
	// far fewer dangling refs than total nodes.
	danglingDetails := enrichDangling(ctx, driver, report.Dangling)

	out, closer, err := c.openOutput()
	if err != nil {
		return err
	}
	defer closer()

	switch c.format {
	case "json":
		if err := writeJSON(out, report, danglingDetails); err != nil {
			return err
		}
	default:
		writeTextSummary(out, report)
	}

	if !report.OK() {
		return fmt.Errorf("store failed integrity check: %d cycle(s), %d dangling ref(s)", len(report.Cycles), len(report.Dangling))
	}
	return nil
}

// openOutput returns the writer for the report and a closer to call when
// done. When --output is empty, it writes to stdout and the closer is a
// no-op.
func (c *validateCommander) openOutput() (io.Writer, func(), error) {
	if c.outputPath == "" {
		return os.Stdout, func() {}, nil
	}
	f, err := os.Create(c.outputPath)
	if err != nil {
		return nil, nil, fmt.Errorf("create output file: %w", err)
	}
	return f, func() { _ = f.Close() }, nil
}

func (c *validateCommander) newStorageDriver(ctx context.Context) (storage.Driver, error) {
	if c.postgresDSN != "" {
		driver, err := postgres.NewDriver(ctx, c.postgresDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to create PostgreSQL driver: %w", err)
		}
		c.logger.Info("using PostgreSQL storage")
		return driver, nil
	}

	driver, err := sqlite.NewDriver(ctx, c.sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SQLite driver: %w", err)
	}
	c.logger.Info("using SQLite storage", "path", c.sqlitePath)
	return driver, nil
}

// danglingDetail is the enriched shape used in JSON output. It carries
// enough context (role, project, content preview, timestamp) to triage a
// dangling reference without a separate sqlite3 query.
type danglingDetail struct {
	Hash           string    `json:"hash"`
	ParentHash     string    `json:"parent_hash"`
	Role           string    `json:"role,omitempty"`
	Type           string    `json:"type,omitempty"`
	Project        string    `json:"project,omitempty"`
	Model          string    `json:"model,omitempty"`
	AgentName      string    `json:"agent_name,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitzero"`
	ContentPreview string    `json:"content_preview,omitempty"`
}

func enrichDangling(ctx context.Context, driver storage.Driver, refs []validate.Dangling) []danglingDetail {
	out := make([]danglingDetail, 0, len(refs))
	for _, r := range refs {
		d := danglingDetail{Hash: r.Hash, ParentHash: r.ParentHash}
		n, err := driver.Get(ctx, r.Hash)
		if err != nil {
			// A missing child would be surprising — it was in the
			// parent-ref scan moments ago — but don't let one bad row
			// sink the whole report.
			out = append(out, d)
			continue
		}
		d.Role = n.Bucket.Role
		d.Type = n.Bucket.Type
		d.Project = n.Project
		d.Model = n.Bucket.Model
		d.AgentName = n.Bucket.AgentName
		d.CreatedAt = n.CreatedAt
		d.ContentPreview = truncatePreview(strings.TrimSpace(n.Bucket.ExtractText()), previewMaxChars)
		out = append(out, d)
	}
	return out
}

func truncatePreview(s string, limit int) string {
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit]) + "…"
}

// jsonReport is the top-level JSON envelope written by --format=json.
type jsonReport struct {
	OK         bool             `json:"ok"`
	TotalNodes int              `json:"total_nodes"`
	Roots      int              `json:"roots"`
	Cycles     []jsonCycle      `json:"cycles"`
	Dangling   []danglingDetail `json:"dangling"`
}

type jsonCycle struct {
	Length int      `json:"length"`
	Path   []string `json:"path"`
}

func writeJSON(w io.Writer, r validate.Report, dangling []danglingDetail) error {
	cycles := make([]jsonCycle, len(r.Cycles))
	for i, c := range r.Cycles {
		cycles[i] = jsonCycle{
			Length: len(c) - 1, // drop the closing repeat from the count
			Path:   c,
		}
	}
	if dangling == nil {
		dangling = []danglingDetail{}
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(jsonReport{
		OK:         r.OK(),
		TotalNodes: r.TotalNodes,
		Roots:      r.Roots,
		Cycles:     cycles,
		Dangling:   dangling,
	})
}

// writeTextSummary prints a single-line test-framework-style PASS/FAIL
// summary. Details are available via --format=json --output file.json.
func writeTextSummary(w io.Writer, r validate.Report) {
	status := "PASS"
	if !r.OK() {
		status = "FAIL"
	}
	fmt.Fprintf(w, "%s  %d nodes · %d roots · %d cycles · %d dangling\n",
		status, r.TotalNodes, r.Roots, len(r.Cycles), len(r.Dangling))
	if !r.OK() {
		fmt.Fprintln(w, "      (use --format=json --output report.json for per-item details)")
	}
}
