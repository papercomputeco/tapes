// Package initcmder provides the init command for initializing a local .tapes
// directory in the current working directory.
package initcmder

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

const (
	dirName = ".tapes"
)

const initLongDesc string = `Initialize a new .tapes/ directory in the current working directory.

Creates a local .tapes/ directory that takes precedence over the default
~/.tapes/ directory for checkout state, storage, configuration,
and other tapes operations.

This is useful for maintaining separate tapes state per project or directory.

Examples:
  tapes init`

const initShortDesc string = "Initialize a local .tapes/ directory"

func NewInitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: initShortDesc,
		Long:  initLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runInit()
		},
	}

	return cmd
}

func runInit() error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getting current directory: %w", err)
	}

	dir := filepath.Join(cwd, dirName)

	info, err := os.Stat(dir)
	if err == nil && info.IsDir() {
		fmt.Printf("Already initialized: %s\n", dir)
		return nil
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating .tapes directory: %w", err)
	}

	fmt.Printf("Initialized .tapes directory: %s\n", dir)
	return nil
}
