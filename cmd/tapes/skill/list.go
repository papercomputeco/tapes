package skillcmder

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/skill"
)

type listCommander struct {
	skillType string
}

func newListCmd() *cobra.Command {
	cmder := &listCommander{}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List skills in ~/.tapes/skills/",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.run(cmd)
		},
	}

	cmd.Flags().StringVar(&cmder.skillType, "type", "", "Filter by skill type")

	return cmd
}

func (c *listCommander) run(cmd *cobra.Command) error {
	w := cmd.OutOrStdout()

	skillsDir, err := skill.SkillsDir()
	if err != nil {
		return err
	}

	skills, err := skill.List(skillsDir)
	if err != nil {
		return fmt.Errorf("list skills: %w", err)
	}

	if len(skills) == 0 {
		fmt.Fprintln(w, "No skills found. Generate one with: tapes skill generate <hash> --name <name>")
		return nil
	}

	// Filter by type if specified
	if c.skillType != "" {
		var filtered []*skill.Skill
		for _, sk := range skills {
			if sk.Type == c.skillType {
				filtered = append(filtered, sk)
			}
		}
		skills = filtered
	}

	if len(skills) == 0 {
		fmt.Fprintf(w, "No skills found with type %q\n", c.skillType)
		return nil
	}

	nameStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("82")).Bold(true)
	typeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	descStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
	tagStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("39"))

	fmt.Fprintf(w, "\nSkills (%d)\n\n", len(skills))

	for _, sk := range skills {
		desc := sk.Description
		if len(desc) > 80 {
			desc = desc[:77] + "..."
		}
		desc = strings.ReplaceAll(desc, "\n", " ")

		fmt.Fprintf(w, "  %s  %s  %s\n",
			nameStyle.Render(sk.Name),
			typeStyle.Render(sk.Type),
			typeStyle.Render("v"+sk.Version),
		)
		fmt.Fprintf(w, "  %s\n", descStyle.Render(desc))
		if len(sk.Tags) > 0 {
			fmt.Fprintf(w, "  %s\n", tagStyle.Render("["+strings.Join(sk.Tags, ", ")+"]"))
		}
		fmt.Fprintln(w)
	}

	return nil
}
