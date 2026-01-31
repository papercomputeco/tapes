package main

import (
	"fmt"
	"os"

	apicmder "github.com/papercomputeco/tapes/cmd/tapes/serve/api"
)

func main() {
	cmd := apicmder.NewAPICmd()

	cmd.Use = "tapesapi"
	cmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging")
	cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")

	err := cmd.Execute()
	if err != nil {
		fmt.Printf("Error executing root command: %v", err)
		os.Exit(1)
	}
}
