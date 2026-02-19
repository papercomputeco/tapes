package skillcmder

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	searchcmder "github.com/papercomputeco/tapes/cmd/tapes/search"
	"github.com/papercomputeco/tapes/cmd/tapes/sqlitepath"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
	"github.com/papercomputeco/tapes/pkg/deck"
	"github.com/papercomputeco/tapes/pkg/dotdir"
	"github.com/papercomputeco/tapes/pkg/skill"
)

type generateCommander struct {
	sqlitePath string
	name       string
	skillType  string
	preview    bool
	provider   string
	model      string
	apiKey     string
	since      string
	until      string
	search     string
	searchTop  int
	apiTarget  string
}

func newGenerateCmd() *cobra.Command {
	cmder := &generateCommander{}

	cmd := &cobra.Command{
		Use:   "generate [hash...]",
		Short: "Extract a skill from conversation(s)",
		Long: `Generate a skill file by extracting reusable patterns from one or
more tapes conversations using an LLM.

Hash resolution (in order):
  1. Positional hash arguments
  2. --search query (searches the API for matching sessions)
  3. Current checkout state (from tapes checkout)

Use --since and --until to filter which conversation turns are included,
like git log --since/--until.

Examples:
  tapes skill generate abc123 --name debug-react-hooks
  tapes skill generate --name my-skill
  tapes skill generate --search "gum glow charm" --name charm-cli-patterns
  tapes skill generate --search "react hooks" --search-top 3 --name react-debug
  tapes skill generate abc123 --name morning-work --since 2026-02-17`,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("api-target") {
				return nil
			}
			configDir, _ := cmd.Flags().GetString("config-dir")
			cfger, err := config.NewConfiger(configDir)
			if err != nil {
				return nil //nolint:nilerr // non-fatal, fall back to default
			}
			cfg, err := cfger.LoadConfig()
			if err != nil {
				return nil //nolint:nilerr // non-fatal, fall back to default
			}
			cmder.apiTarget = cfg.Client.APITarget
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmder.run(cmd, args)
		},
	}

	defaults := config.NewDefaultConfig()

	cmd.Flags().StringVar(&cmder.name, "name", "", "Skill name, kebab-case (required)")
	cmd.Flags().StringVar(&cmder.skillType, "type", "workflow", "Skill type: workflow|domain-knowledge|prompt-template")
	cmd.Flags().BoolVar(&cmder.preview, "preview", false, "Show generated skill without saving")
	cmd.Flags().StringVar(&cmder.provider, "provider", "openai", "LLM provider (openai|anthropic|ollama)")
	cmd.Flags().StringVar(&cmder.model, "model", "", "LLM model for extraction")
	cmd.Flags().StringVar(&cmder.apiKey, "api-key", "", "API key for LLM provider")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database")
	cmd.Flags().StringVar(&cmder.since, "since", "", "Only include messages on or after this date (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.until, "until", "", "Only include messages on or before this date (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.search, "search", "", "Search query to find sessions (requires running API server)")
	cmd.Flags().IntVar(&cmder.searchTop, "search-top", 3, "Number of search results to use")
	cmd.Flags().StringVar(&cmder.apiTarget, "api-target", defaults.Client.APITarget, "Tapes API server URL")

	_ = cmd.MarkFlagRequired("name")

	return cmd
}

func (c *generateCommander) run(cmd *cobra.Command, args []string) error {
	w := cmd.OutOrStdout()

	hashes, err := c.resolveHashes(cmd, args)
	if err != nil {
		return err
	}

	if !skill.ValidSkillType(c.skillType) {
		return fmt.Errorf("invalid --type %q; valid types: %s", c.skillType, strings.Join(skill.SkillTypes, ", "))
	}

	opts, err := c.buildGenerateOptions()
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "\nGenerating skill %q from %d conversation(s)\n\n", c.name, len(hashes))

	// Step 1: Open database
	var query *deck.Query
	var closeFn func() error
	if err := step(w, "Opening database", func() error {
		dbPath, dbErr := sqlitepath.ResolveSQLitePath(c.sqlitePath)
		if dbErr != nil {
			return dbErr
		}
		query, closeFn, dbErr = deck.NewQuery(cmd.Context(), dbPath, nil)
		return dbErr
	}); err != nil {
		return err
	}
	defer func() { _ = closeFn() }()

	// Step 2: Configure LLM
	var llmCaller deck.LLMCallFunc
	if err := step(w, "Configuring LLM provider", func() error {
		credMgr, credErr := credentials.NewManager("")
		if credErr != nil {
			return fmt.Errorf("loading credentials: %w", credErr)
		}
		var llmErr error
		llmCaller, llmErr = deck.NewLLMCaller(deck.LLMCallerConfig{
			Provider: c.provider,
			Model:    c.model,
			APIKey:   c.apiKey,
			CredMgr:  credMgr,
		})
		return llmErr
	}); err != nil {
		return err
	}

	// Step 3: Extract skill via LLM
	gen := skill.NewGenerator(query, llmCaller)
	var sk *skill.Skill
	if err := step(w, "Extracting skill from session transcript(s)", func() error {
		var genErr error
		sk, genErr = gen.Generate(cmd.Context(), hashes, c.name, c.skillType, opts)
		return genErr
	}); err != nil {
		return err
	}

	// Render the generated SKILL.md through glamour
	fmt.Fprintln(w)
	md := skill.RenderSkillMD(sk)
	rendered, err := renderMarkdown(md)
	if err != nil {
		// Fall back to plain text if glamour fails
		fmt.Fprintln(w, md)
	} else {
		fmt.Fprint(w, rendered)
	}

	if c.preview {
		return nil
	}

	// Step 4: Write to disk
	var path string
	if err := step(w, "Writing SKILL.md", func() error {
		skillsDir, dirErr := skill.SkillsDir()
		if dirErr != nil {
			return dirErr
		}
		var writeErr error
		path, writeErr = skill.Write(sk, skillsDir)
		return writeErr
	}); err != nil {
		return err
	}

	fmt.Fprintf(w, "\n  Saved to %s\n", path)
	fmt.Fprintf(w, "  Run 'tapes skill sync %s' to install for Claude Code\n\n", c.name)
	return nil
}

// resolveHashes determines conversation hashes from args, --search, or checkout.
func (c *generateCommander) resolveHashes(cmd *cobra.Command, args []string) ([]string, error) {
	// 1. Positional args take priority
	if len(args) > 0 {
		return args, nil
	}

	// 2. --search query
	if c.search != "" {
		return c.searchForHashes(cmd)
	}

	// 3. Fall back to current checkout
	mgr := dotdir.NewManager()
	state, err := mgr.LoadCheckoutState("")
	if err != nil {
		return nil, fmt.Errorf("loading checkout state: %w", err)
	}
	if state == nil {
		return nil, errors.New("no hashes provided, no --search query, and no checkout state;\nprovide a hash, use --search, or run 'tapes checkout <hash>' first")
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Using current checkout: %s\n", state.Hash)
	return []string{state.Hash}, nil
}

func (c *generateCommander) searchForHashes(cmd *cobra.Command) ([]string, error) {
	fmt.Fprintf(cmd.OutOrStdout(), "Searching for %q...\n", c.search)

	output, err := searchcmder.SearchAPI(c.apiTarget, c.search, c.searchTop)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	if output.Count == 0 {
		return nil, fmt.Errorf("no sessions found for search %q", c.search)
	}

	var hashes []string
	for _, result := range output.Results {
		hash := searchcmder.LeafHash(result)
		fmt.Fprintf(cmd.OutOrStdout(), "  found: %s (score: %.4f)\n", hash, result.Score)
		hashes = append(hashes, hash)
	}

	return hashes, nil
}

func (c *generateCommander) buildGenerateOptions() (*skill.GenerateOptions, error) {
	if c.since == "" && c.until == "" {
		return nil, nil
	}

	opts := &skill.GenerateOptions{}

	if c.since != "" {
		t, err := parseTime(c.since)
		if err != nil {
			return nil, fmt.Errorf("invalid --since: %w", err)
		}
		opts.Since = &t
	}

	if c.until != "" {
		t, err := parseTime(c.until)
		if err != nil {
			return nil, fmt.Errorf("invalid --until: %w", err)
		}
		opts.Until = &t
	}

	return opts, nil
}

func parseTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errors.New("empty time")
	}

	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}

	if parsed, err := time.Parse("2006-01-02", value); err == nil {
		return parsed, nil
	}

	return time.Time{}, errors.New("expected RFC3339 or YYYY-MM-DD")
}
