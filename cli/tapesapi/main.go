package main

import (
	"os"

	apicmder "github.com/papercomputeco/tapes/cmd/tapes/serve/api"
)

func main() {
	cmd := apicmder.NewAPICmd()
	cmd.Use = "tapesapi"
	cmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
