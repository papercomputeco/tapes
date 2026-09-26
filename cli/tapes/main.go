package main

import (
	"os"

	tapescmder "github.com/papercomputeco/tapes/cmd/tapes"
)

func main() {
	cmd := tapescmder.NewTapesCmd()
	// cobra already printed the error to stderr; a second copy on stdout
	// would land in whatever a script captured.
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
