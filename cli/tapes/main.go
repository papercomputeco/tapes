package main

import (
	"fmt"
	"os"

	tapescmder "github.com/papercomputeco/tapes/cmd/tapes"
)

func main() {
	cmd := tapescmder.NewTapesCmd()
	err := cmd.Execute()
	if err != nil {
		fmt.Printf("Error executing root command: %v\n", err)
		os.Exit(1)
	}
}
