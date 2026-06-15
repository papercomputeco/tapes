package skillcmder

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/inprocessapi"
	searchcmder "github.com/papercomputeco/tapes/cmd/tapes/search"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
	"github.com/papercomputeco/tapes/pkg/skill"
)

type generateCommander struct {
	flags config.FlagSet

	postgresDSN string
	name        string
	skillType   string
	preview     bool
	provider    string
	model       string
	apiKey      string
	since       string
	until       string
	search      string
	searchTop   int
	apiTarget   string
}

var generateFlags = config.FlagSet{
	config.FlagAPITarget: {Name: "api-target", ViperKey: "client.api_target", Description: "Tapes API server URL"},
}

func newGenerateCmd() *cobra.Command {
	cmder := &generateCommander{
		flags: generateFlags,
	}

	cmd := &cobra.Command{
		Use:   "generate [session-id...]",
		Short: "Extract a skill from session(s)",
		Long: `Generate a skill file by extracting reusable patterns from one or
more tapes sessions using an LLM.

Session resolution (in order):
  1. Positional session ID arguments (/v1/sessions UUIDs)
  2. --search query (span search for matching sessions)

The transcript is built from the session's trace/span projection:
turn-grain prompt/response pairs from the conversation spine, with
harness shadow calls (permission checks, title-gen, injected context)
excluded.

Use --since and --until to filter which turns are included,
like git log --since/--until.

Examples:
  tapes skill generate 0196fdb1-93f4-7c41-a53d-0fbe2c5e1f23 --name debug-react-hooks
  tapes skill generate --name my-skill
  tapes skill generate --search "gum glow charm" --name charm-cli-patterns
  tapes skill generate --search "react hooks" --search-top 3 --name react-debug
  tapes skill generate <session-id> --name morning-work --since 2026-02-17`,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return nil //nolint:nilerr // non-fatal, fall back to default
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagAPITarget,
			})

			cmder.apiTarget = v.GetString("client.api_target")
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmder.run(cmd, args)
		},
	}

	cmd.Flags().StringVar(&cmder.name, "name", "", "Skill name, kebab-case (required)")
	cmd.Flags().StringVar(&cmder.skillType, "type", "workflow", "Skill type: workflow|domain-knowledge|prompt-template")
	cmd.Flags().BoolVar(&cmder.preview, "preview", false, "Show generated skill without saving")
	cmd.Flags().StringVar(&cmder.provider, "provider", "openai", "LLM provider (openai|anthropic|ollama)")
	cmd.Flags().StringVar(&cmder.model, "model", "", "LLM model for extraction")
	cmd.Flags().StringVar(&cmder.apiKey, "api-key", "", "API key for LLM provider")
	cmd.Flags().StringVar(&cmder.postgresDSN, "postgres", "", "PostgreSQL connection string for a local in-process API")
	cmd.Flags().StringVar(&cmder.since, "since", "", "Only include turns on or after this date (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.until, "until", "", "Only include turns on or before this date (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.search, "search", "", "Search query to find sessions (requires running API server)")
	cmd.Flags().IntVar(&cmder.searchTop, "search-top", 3, "Number of search results to use")
	config.AddStringFlag(cmd, cmder.flags, config.FlagAPITarget, &cmder.apiTarget)

	_ = cmd.MarkFlagRequired("name")

	return cmd
}

func (c *generateCommander) run(cmd *cobra.Command, args []string) error {
	w := cmd.OutOrStdout()

	sessionIDs, err := c.resolveSessionIDs(cmd, args)
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

	fmt.Fprintf(w, "\nGenerating skill %q from %d session(s)\n\n", c.name, len(sessionIDs))

	// Step 1: Connect to the tapes API. With --api-target we use the
	// remote server; otherwise we spin up an in-process API server backed
	// by --postgres.
	var query skill.Querier
	var closeFn func()
	if err := step(w, "Connecting to API", func() error {
		if strings.TrimSpace(c.apiTarget) != "" {
			query = skill.NewAPIClient(c.apiTarget)
			closeFn = func() {}
			return nil
		}

		if strings.TrimSpace(c.postgresDSN) == "" {
			return errors.New("no API target configured: pass --api-target or --postgres")
		}
		target, stop, startErr := inprocessapi.Start(cmd.Context(), c.postgresDSN, nil)
		if startErr != nil {
			return startErr
		}
		query = skill.NewAPIClient(target)
		closeFn = stop
		return nil
	}); err != nil {
		return err
	}
	defer closeFn()

	// Step 2: Configure LLM
	var llmCaller skill.LLMCallFunc
	if err := step(w, "Configuring LLM provider", func() error {
		credMgr, credErr := credentials.NewManager("")
		if credErr != nil {
			return fmt.Errorf("loading credentials: %w", credErr)
		}
		var llmErr error
		llmCaller, llmErr = skill.NewLLMCaller(skill.LLMCallerConfig{
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
		sk, genErr = gen.Generate(cmd.Context(), sessionIDs, c.name, c.skillType, opts)
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

// resolveSessionIDs determines session IDs from args or --search.
func (c *generateCommander) resolveSessionIDs(cmd *cobra.Command, args []string) ([]string, error) {
	// 1. Positional args take priority
	if len(args) > 0 {
		return args, nil
	}

	// 2. --search query
	if c.search != "" {
		return c.searchForSessions(cmd)
	}

	return nil, errors.New("no session IDs provided and no --search query;\nprovide a session ID or use --search")
}

// searchForSessions resolves --search via the span search API: each
// hit carries the session it belongs to, deduplicated in score order.
func (c *generateCommander) searchForSessions(cmd *cobra.Command) ([]string, error) {
	fmt.Fprintf(cmd.OutOrStdout(), "Searching for %q...\n", c.search)

	output, err := searchcmder.SearchSpansAPI(c.apiTarget, c.search, "", c.searchTop)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	var sessionIDs []string
	seen := map[string]bool{}
	for _, result := range output.Results {
		if result.SessionID == "" || seen[result.SessionID] {
			continue
		}
		seen[result.SessionID] = true
		fmt.Fprintf(cmd.OutOrStdout(), "  found: %s (score: %.4f)\n", result.SessionID, result.Score)
		sessionIDs = append(sessionIDs, result.SessionID)
	}

	if len(sessionIDs) == 0 {
		return nil, fmt.Errorf("no sessions found for search %q", c.search)
	}

	return sessionIDs, nil
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
