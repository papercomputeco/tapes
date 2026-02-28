package foocmder

import (
	"fmt"

	"github.com/spf13/cobra"
)

const fooLongDesc string = `Print a simple "foo" greeting message.

Examples:
  tapes foo
  tapes foo --bar`

const fooShortDesc string = "Print foo"

type fooCommander struct {
	bar bool
}

func NewFooCmd() *cobra.Command {
	cmder := &fooCommander{}

	cmd := &cobra.Command{
		Use:   "foo",
		Short: fooShortDesc,
		Long:  fooLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return cmder.run()
		},
	}

	cmd.Flags().BoolVarP(&cmder.bar, "bar", "b", false, "Include bar in output")

	return cmd
}

func (c *fooCommander) run() error {
	if c.bar {
		fmt.Println("foobar")
		return nil
	}
	fmt.Println("foo")
	return nil
}
