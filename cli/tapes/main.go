package main

import (
	"os"

	tapescmder "github.com/papercomputeco/tapes/cmd/tapes"
)

func main() {
	cmd := tapescmder.NewTapesCmd()
	// The error is already on stderr.
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
