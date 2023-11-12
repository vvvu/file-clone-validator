package datasource

import (
	"context"
	"file-clone-validator/core/metadata"
	"file-clone-validator/core/utils"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"os"
	"path/filepath"
)

type FileSource struct {
	root string
}

type FileItem struct {
	Path string
	Info os.FileInfo
}

// NewFileSource creates a new FileSource which is a DataSource implementation that reads files from the file system.
// Input:
// - root: the root directory to read files from
func NewFileSource(root string) (DataSource, error) {
	rootPath, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return &FileSource{root: rootPath}, nil
}

// Walk walks the file system and sends the metadata of each file to the given channel. 1 scanner goroutine will walk
// the file system and send the file paths to the N worker goroutines. The N worker goroutines will retrieve the
// metadata of the file and send it to the output channel.
// Input:
// - outDir: the directory to save the metadata files to. This directory should be empty and needs to be filtered out
// - out: the channel to send the metadata to
// - workerCount: the number of workers to use to retrieve the metadata
// Note:
// - There are two types of goroutine in this function:
// --- Scanner: walks the file system and sends the file paths to the worker goroutines
// --- Worker: retrieves the metadata of the file and sends it to the output channel
// The concurrency of Scanner is 1 because we're limited by the function filepath.Walk. Multiple scanners may cause
// data races and make the code more complex. The concurrency of Worker is workerCount. Workers deal with the metadata
// and IO operations which are more expensive than the file path operations of the Scanner. Therefore, we can have
// multiple workers to improve the performance.
func (fs *FileSource) Walk(ctx context.Context, outDir string, out chan<- *metadata.Meta, workerCount int) error {
	slog.Info("Start walking the file system:", slog.Int("WorkerCount", workerCount))
	defer close(out) // close the output channel when done

	outputTempPath, err := utils.GetTempPath(outDir)
	if err != nil {
		return err
	}

	itemC := make(chan *FileItem, 1)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { // scanner goroutine
		defer close(itemC) // to notify the workers that there are no more items to process
		return filepath.Walk(fs.root, func(path string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// filter paths
			if path == fs.root { // skip the root directory
				return nil
			}

			isTemp, err := utils.IsSubPath(outputTempPath, path)
			if err != nil {
				return err
			}

			if isTemp { // skip the temp directory
				return nil
			}
			// end of filter paths

			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case itemC <- &FileItem{Path: path, Info: fi}:
			}
			return nil
		})
	})

	for i := 0; i < workerCount; i++ {
		group.Go(func() error { // worker goroutines
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case item, ok := <-itemC:
					if !ok {
						return nil
					}

					// retrieve the metadata of the file
					meta, err := metadata.RetrieveFileSystemMeta(item.Path, item.Info)
					if err != nil { // to make sure that the fbs is retrieved, we will handle the error the first time
						return err
					}

					select {
					case <-groupCtx.Done():
						return groupCtx.Err()
					case out <- meta:
					}
				}
			}
		})
	}

	return group.Wait()
}
