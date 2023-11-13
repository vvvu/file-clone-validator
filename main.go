package main

import (
	"file-clone-validator/cmd"
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

func main() {
	rootCmd := &cobra.Command{Use: "validator"}
	rootCmd.AddCommand(cmd.GenerateCmd)
	rootCmd.AddCommand(cmd.ValidateCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to execute command: %v\n", err)
		os.Exit(1)
	}
}
