package cmd

import (
	"context"
	"file-clone-validator/core/datasource"
	"file-clone-validator/core/metadata"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"os"
	"testing"
)

func TestGenerateFileSystem(t *testing.T) {
	srcDir := "./"
	outDir := "./"
	ds, err := datasource.NewFileSource(srcDir)
	require.NoError(t, err)

	writer, err := datasource.NewMetaWriter(srcDir, outDir)
	require.NoError(t, err)

	metaItemC := make(chan *metadata.Meta, 1)
	g, gCtx := errgroup.WithContext(context.Background())
	g.Go(func() error { return ds.Walk(gCtx, outDir, metaItemC, 1) })
	g.Go(func() error { return writer.Write(gCtx, metaItemC, 1) })
	require.NoError(t, g.Wait())

	require.NoError(t, os.Remove("./meta.out"))
}
