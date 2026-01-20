package main

import (
	"os"

	tapescmder "github.com/papercomputeco/tapes/cmd/tapes"
)

func main() {
	cmd := tapescmder.NewTapesCmd()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
