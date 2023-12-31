package datasource

import (
	"bufio"
	"context"
	"encoding/json"
	"file-clone-validator/core/metadata"
	"github.com/cheggaaa/pb/v3"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DataSource indicates the source of the data before it is copied to the destination
type DataSource interface {
	// Walk walks the underlying storage and sends the metadata of each file to the given channel
	// Input:
	// - outDir: the output directory of the metadata file. This directory should be used to store temporary files and
	// 		 the final metadata file. The output directory will be filtered and will not be included in the metadata
	// - out: the channel to send the metadata to
	// - workerCount: the number of workers to use to retrieve the metadata
	Walk(ctx context.Context, outDir string, out chan<- *metadata.Meta, workerCount int) error
}

// MetaHeader is the header of the output metadata file
type MetaHeader struct {
	// SourceDir is the root directory of the source
	SourceDir string

	// ItemCount is the number of items in the metadata file
	ItemCount uint64
}

// MetaWriter is the interface that writes the metadata to the output file
type MetaWriter interface {
	// Write writes the metadata to the output file.
	// Input:
	// - in: the channel to read the metadata from
	// - workerCount: the number of workers to use to write the metadata
	Write(ctx context.Context, in <-chan *metadata.Meta, workerCount int) error
}

func NewMetaWriter(srcDir, outDir string) (MetaWriter, error) {
	srcDir, err := filepath.Abs(srcDir)
	if err != nil {
		return nil, err
	}

	outDir, err = filepath.Abs(outDir)
	if err != nil {
		return nil, err
	}

	writer := &MetaWriterImpl{
		SourceDir:     srcDir,
		OutputDir:     outDir,
		OutputTempDir: filepath.Join(outDir, "temp_dir"),
		ItemCount:     0,
	}

	err = os.RemoveAll(writer.OutputTempDir)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(outDir, 0700)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(writer.OutputTempDir, 0700)
	if err != nil {
		return nil, err
	}

	slog.Info("Success to create meta writer:", slog.Any("MetaWriter", writer))
	return writer, nil
}

type MetaWriterImpl struct {
	SourceDir     string
	OutputDir     string
	OutputTempDir string
	ItemCount     uint64
}

func (w *MetaWriterImpl) Write(ctx context.Context, in <-chan *metadata.Meta, workerCount int) error {
	defer os.RemoveAll(w.OutputTempDir)

	slog.Info("Start to write metadata to temp file:", slog.String("OutputDir", w.OutputDir))

	itemCounts := make([]uint64, workerCount) // to store the number of items written by each worker

	group, groupCtx := errgroup.WithContext(ctx)

	go GenerateProgressWatch(groupCtx, itemCounts)
	// monitor the progress of the metadata generation
	// the lifetime of the progress bar is the same as the lifetime of the metadata generation

	for i := 0; i < workerCount; i++ {
		_i := i
		group.Go(func() error {
			defer func() { w.ItemCount += itemCounts[_i] }()
			tempFile, err := os.CreateTemp(w.OutputTempDir, "temp-*")
			if err != nil {
				return err
			}
			defer tempFile.Close()

			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case meta, ok := <-in:
					if !ok {
						return nil
					}

					data, _err := metadata.Serialise(meta)
					if _err != nil {
						return _err
					}

					_, _err = tempFile.WriteString(string(data) + "\n")
					if _err != nil {
						return _err
					}

					itemCounts[_i]++
				}
			}
		})
	}
	err := group.Wait()
	if err != nil {
		return err
	}

	slog.Info("Finish to write metadata to temp output file:", slog.String("OutputDir", w.OutputDir))

	slog.Info("Start to merge temp files to final output:", slog.String("OutputDir", w.OutputDir))

	outFile, err := os.Create(filepath.Join(w.OutputDir, "meta.out"))
	if err != nil {
		return err
	}
	defer outFile.Close()

	header := &MetaHeader{
		SourceDir: w.SourceDir,
		ItemCount: w.ItemCount,
	}

	// write header first
	headerData, err := json.Marshal(header)
	if err != nil {
		return err
	}
	_, err = outFile.WriteString(string(headerData) + "\n")
	if err != nil {
		return err
	}

	// merge all temp files to final output
	err = filepath.Walk(w.OutputTempDir, func(fp string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasPrefix(info.Name(), "temp-") {
			tempFile, _err := os.Open(fp)
			if _err != nil {
				return _err
			}
			defer func() {
				tempFile.Close()
			}()

			scanner := bufio.NewScanner(tempFile)
			for scanner.Scan() {
				_, _err = outFile.WriteString(scanner.Text() + "\n")
				if _err != nil {
					return _err
				}
			}

			if _err = scanner.Err(); _err != nil {
				return _err
			}
			return nil
		}

		return nil
	})

	slog.Info("Finish to merge temp files to final output:", slog.String("OutputDir", w.OutputDir))
	return err
}

// GenerateProgressWatch generates a progress bar to watch the progress of the metadata generation
// Input:
// - itemCounts: the number of items written by each worker. This function will not modify the values of this slice
// but will read the values to calculate the total number of items written
func GenerateProgressWatch(ctx context.Context, itemCounts []uint64) {
	bar := pb.New64(0) // `0` for indefinite mode
	bar.Start()
	defer bar.Finish()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			var total uint64
			for _, itemCount := range itemCounts {
				total += itemCount
			}
			bar.SetCurrent(int64(total))
			return
		case <-ticker.C:
			var total uint64
			for _, itemCount := range itemCounts {
				total += itemCount
			}
			bar.SetCurrent(int64(total))
		}
	}
}
