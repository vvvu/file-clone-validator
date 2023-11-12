package cmd

import (
	"context"
	"errors"
	"file-clone-validator/core/validator"
	"fmt"
	"github.com/spf13/cobra"
	"log/slog"
)

var (
	targetDir      string
	metaFilePath   string
	validateType   SourceType
	validatorCount int

	ValidateCmd = &cobra.Command{
		Use:   "validate",
		Short: "Validate the metadata file",
		Long:  "Validate a metadata file with the specified target directory or storage bucket",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if targetDir == "" || metaFilePath == "" {
				return fmt.Errorf("target directory and metadata file path must be specified. "+
					"got target directory: %s, metadata file path: %s", targetDir, metaFilePath)
			}

			if validatorCount < 1 {
				return fmt.Errorf("validator count must be greater than 0. got %d", validatorCount)
			}

			if validateType != FS && validateType != OSS {
				return fmt.Errorf("invalid source type: %s. expect [fs|oss]", validateType)
			}

			slog.Info("Finish to validate flags:",
				slog.String("TargetDir", targetDir),
				slog.String("MetaFilePath", metaFilePath),
				slog.String("SourceType", string(validateType)),
				slog.Int("ValidatorCount", validatorCount),
			)

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			reporter, err := validator.NewReporter("./error_report.txt")
			if err != nil {
				return fmt.Errorf("failed to create reporter: %w", err)
			}
			defer reporter.Flush()

			ctx := context.Background()
			switch validateType {
			case FS:
				v, err := validator.NewFileValidator(targetDir, reporter)
				if err != nil {
					return fmt.Errorf("failed to create file validator: %w", err)
				}

				return v.Validate(ctx, metaFilePath, validatorCount)
			case OSS:
			default:
				return errors.New("not implemented yet")
			}

			return nil
		},
	}
)

func initValidateCmd() {
	ValidateCmd.PersistentFlags().StringVarP(&targetDir, "target", "t", "", "the target directory or storage bucket")
	ValidateCmd.PersistentFlags().StringVarP(&metaFilePath, "meta", "m", "", "the metadata file path")
	ValidateCmd.PersistentFlags().StringVarP((*string)(&validateType), "type", "y", "fs", "the type of the target. [fs|oss]")
	ValidateCmd.PersistentFlags().IntVarP(&validatorCount, "validator", "v", 16, "the number of validators to use")
}
