package main

import (
	"context"
	"file-clone-validator/core/datasource"
	"file-clone-validator/core/metadata"
	"golang.org/x/sync/errgroup"
)

func main() {
	srcDir := "./"
	outDir := "./"
	ds, err := datasource.NewFileSource(srcDir)
	if err != nil {
		panic(err)
	}
	metaC := make(chan *metadata.Meta)
	group, groupCtx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		return ds.Walk(groupCtx, outDir, metaC, 1)
	})

	writer, err := datasource.NewMetaWriter(srcDir, outDir)
	if err != nil {
		panic(err)
	}
	group.Go(func() error {
		return writer.Write(groupCtx, metaC, 1)
	})

	if err = group.Wait(); err != nil {
		panic(err)
	}

}
