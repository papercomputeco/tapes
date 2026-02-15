package main

import (
	"fmt"
	"os"

	proxycmder "github.com/papercomputeco/tapes/cmd/tapes/serve/proxy"
)

func main() {
	cmd := proxycmder.NewProxyCmd()

	cmd.Use = "tapesproxy"
	cmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging")
	cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")

	err := cmd.Execute()
	if err != nil {
		fmt.Printf("Error executing root command: %v\n", err)
		os.Exit(1)
	}
}
