package validator

import (
	"bufio"
	"context"
	"encoding/json"
	"file-clone-validator/core/datasource"
	"file-clone-validator/core/metadata"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileValidator struct {
	targetDir string
	reporter  *Reporter
}

func NewFileValidator(targetDir string, reporter *Reporter) (Validator, error) {
	targetDir, err := filepath.Abs(targetDir)
	if err != nil {
		return nil, err
	}

	if _, err = os.Stat(targetDir); err != nil {
		return nil, fmt.Errorf("failed to stat target directory: %w", err)
	}
	return &FileValidator{targetDir: targetDir, reporter: reporter}, nil
}

func (fv *FileValidator) Validate(ctx context.Context, filePath string, workerCount int) error {
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		return err
	}
	metaFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer metaFile.Close()

	s := bufio.NewScanner(metaFile)

	// unmarshal the first line of the metadata file to srcHeader
	srcHeader := datasource.MetaHeader{}
	for s.Scan() {
		err = json.Unmarshal([]byte(s.Text()), &srcHeader)
		if err != nil {
			return err
		}
		break
	}

	itemCounts := make([]uint64, workerCount)

	slog.Info("Start to validate metadata file:", slog.String("MetaFilePath", filePath))

	// validate the metadata file
	rowC := make(chan []byte, 1)
	group, groupCtx := errgroup.WithContext(ctx)

	go ValidateProgressWatch(groupCtx, int64(srcHeader.ItemCount), itemCounts)

	group.Go(func() error {
		defer close(rowC)
		for s.Scan() {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case rowC <- []byte(s.Text()):
			}
		}
		return s.Err()
	})

	for i := 0; i < workerCount; i++ {
		_i := i
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case row, ok := <-rowC:
					if !ok {
						return nil
					}

					item := metadata.Meta{}
					err = json.Unmarshal(row, &item)
					if err != nil {
						fv.reporter.Record("InvalidJSON", fmt.Errorf("source: %s, error: %s", string(row), err.Error()))
						continue
					}

					itemCounts[_i]++

					targetPath := strings.Replace(item.Common.Path, srcHeader.SourceDir, fv.targetDir, 1)
					file, _err := os.Open(targetPath)
					if _err != nil {
						fv.reporter.Record("FileNotFound", fmt.Errorf("source: %s, error: %s", string(row), _err.Error()))
						continue
					}

					fileStat, err := file.Stat()
					if err != nil {
						fv.reporter.Record("FileStatError", fmt.Errorf("source: %s, error: %s", string(row), err.Error()))
						continue
					}
					file.Close()

					targetItem, err := metadata.RetrieveFileSystemMeta(targetPath, fileStat)
					if err != nil {
						fv.reporter.Record("RetrieveMetaFail", fmt.Errorf("source: %s, error: %s", string(row), err.Error()))
						continue
					}

					reasons := item.Equals(targetItem)
					if len(reasons) > 0 {
						fv.reporter.Record("MetaMismatch", fmt.Errorf("source: %s, error: %s", string(row), strings.Join(reasons, ",")))
						continue
					}
				}
			}
		})
	}
	err = group.Wait()
	if err != nil {
		return err
	}

	slog.Info("Finish to validate metadata file:", slog.String("MetaFilePath", filePath))

	var totalCount uint64
	for _, itemCount := range itemCounts {
		totalCount += itemCount
	}
	if totalCount != srcHeader.ItemCount {
		return fmt.Errorf("item count mismatch. expect %d, got %d", srcHeader.ItemCount, totalCount)
	}

	return nil
}

func ValidateProgressWatch(ctx context.Context, total int64, itemCounts []uint64) {
	bar := pb.New64(total)
	bar.Start()
	defer bar.Finish()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			var totalCount uint64
			for _, itemCount := range itemCounts {
				totalCount += itemCount
			}
			bar.SetCurrent(int64(totalCount))
			return
		case <-ticker.C:
			var totalCount uint64
			for _, itemCount := range itemCounts {
				totalCount += itemCount
			}
			bar.SetCurrent(int64(totalCount))
		}
	}
}
