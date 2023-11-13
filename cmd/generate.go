package cmd

import (
	"context"
	"errors"
	"file-clone-validator/core/datasource"
	"file-clone-validator/core/metadata"
	"fmt"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"log/slog"
)

type SourceType string

const (
	FS  SourceType = "fs"
	OSS SourceType = "oss"
)

var (
	sourceDir    string
	outputDir    string
	generateType SourceType
	readerCount  int
	writerCount  int

	GenerateCmd = &cobra.Command{
		Use:     "generate",
		Short:   "Generate metadata file from a data source",
		Long:    "Generate a metadata file from the specified source directory or storage bucket",
		Example: "./binary generate --source ./ --output ./output --type fs --reader 16 --writer 16",
		PreRunE: func(cmd *cobra.Command, args []string) error { // pre run to validate flags
			if sourceDir == "" || outputDir == "" {
				return fmt.Errorf("source and output directory must be specified. "+
					"got source: %s, output: %s", sourceDir, outputDir)
			}

			if readerCount < 1 {
				return fmt.Errorf("reader count must be greater than 0. got %d", readerCount)
			}

			if writerCount < 1 {
				return fmt.Errorf("writer count must be greater than 0. got %d", writerCount)
			}

			if generateType != FS && generateType != OSS {
				return fmt.Errorf("invalid source type: %s. expect [fs|oss]", generateType)
			}

			slog.Info("Finish to validate flags:",
				slog.String("SourceDir", sourceDir),
				slog.String("OutputDir", outputDir),
				slog.String("SourceType", string(generateType)),
				slog.Int("ReaderCount", readerCount),
				slog.Int("WriterCount", writerCount),
			)

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			switch generateType {
			case FS:
				ds, err := datasource.NewFileSource(sourceDir)
				if err != nil {
					return fmt.Errorf("failed to create file source: %w", err)
				}

				writer, err := datasource.NewMetaWriter(sourceDir, outputDir)
				if err != nil {
					return fmt.Errorf("failed to create meta writer: %w", err)
				}

				metaItemC := make(chan *metadata.Meta, 1)
				g, gCtx := errgroup.WithContext(ctx)
				g.Go(func() error { return ds.Walk(gCtx, outputDir, metaItemC, readerCount) })
				g.Go(func() error { return writer.Write(gCtx, metaItemC, writerCount) })
				if err := g.Wait(); err != nil {
					return fmt.Errorf("failed to generate metadata: %w", err)
				}
			case OSS:
			default:
				return errors.New("not implemented yet")
			}

			return nil
		},
	}
)

func initGenerateCmd() {
	GenerateCmd.PersistentFlags().StringVarP(&sourceDir, "source", "s", "", "source directory or storage bucket name")
	GenerateCmd.PersistentFlags().StringVarP(&outputDir, "output", "o", "", "output directory path")
	GenerateCmd.PersistentFlags().IntVarP(&readerCount, "reader", "r", 1, "number of reader to open and load file meta")
	GenerateCmd.PersistentFlags().IntVarP(&writerCount, "writer", "w", 1, "number of writer to write meta to file")
	GenerateCmd.PersistentFlags().StringVarP((*string)(&generateType), "type", "t", "fs", "type of data source to use")
}
