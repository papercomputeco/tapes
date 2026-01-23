// Package versioncmder
package versioncmder

import (
	"fmt"

	"github.com/papercomputeco/tapes/pkg/utils"
	"github.com/spf13/cobra"
)

type VersionCommander struct {
	semVer string
	commit string
}

func NewVersionCmd() *cobra.Command {
	cmder := &VersionCommander{}

	cmd := &cobra.Command{
		Use:   "version",
		Short: "displays version",
		Long:  "displays the version of this CLI",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.run()
		},
	}

	return cmd
}

func (c *VersionCommander) run() error {
	fmt.Printf("Version: %s\nSha: %s\nBuilt at: %s\n", utils.Version, utils.Sha, utils.Buildtime)
	return nil
}
