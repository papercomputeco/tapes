package main

import (
	"os"

	proxycmder "github.com/papercomputeco/tapes/cmd/tapes/serve/proxy"
)

func main() {
	cmd := proxycmder.NewProxyCmd()
	cmd.Use = "tapesproxy"
	cmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
