package main

import (
	"file-clone-validator/cmd"
	"github.com/spf13/cobra"
	"log/slog"
	"os"
)

func main() {
	rootCmd := &cobra.Command{Use: "validator"}
	rootCmd.AddCommand(cmd.GenerateCmd)
	rootCmd.AddCommand(cmd.ValidateCmd)

	if err := rootCmd.Execute(); err != nil {
		slog.Error("Failed to execute command:", slog.Any("error", err))
		os.Exit(1)
	}
}
