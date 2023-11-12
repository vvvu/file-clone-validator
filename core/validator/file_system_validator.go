package validator

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"file-clone-validator/core/datasource"
	"file-clone-validator/core/metadata"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

	itemCount := uint64(0)

	slog.Info("Start to validate metadata file:", slog.String("MetaFilePath", filePath))
	bar := pb.New64(int64(srcHeader.ItemCount))
	bar.Start()

	// validate the metadata file
	rowC := make(chan []byte, 1)
	group, groupCtx := errgroup.WithContext(ctx)
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
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case row, ok := <-rowC:
					if !ok {
						return nil
					}
					bar.Increment()

					item := metadata.Meta{}
					err = json.Unmarshal(row, &item)
					if err != nil {
						fv.reporter.Record("InvalidJSON", fmt.Errorf("source: %s, error: %s", string(row), err.Error()))
						continue
					}

					atomic.AddUint64(&itemCount, 1)

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

	bar.Finish()
	slog.Info("Finish to validate metadata file:", slog.String("MetaFilePath", filePath))

	if itemCount != srcHeader.ItemCount {
		return errors.New("item count mismatch")
	}

	return nil
}
