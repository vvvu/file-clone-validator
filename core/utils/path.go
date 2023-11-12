package utils

import (
	"path/filepath"
	"strings"
)

var (
	outputFile = "meta.out"
)

const (
	TempDir = "temp_dir"
)

func SetOutputFileName(name string) {
	outputFile = name
}

func GetOutputFileName() string {
	return outputFile
}

func GetAbsolutePath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	return filepath.Abs(path)
}

func GetTempPath(outDir string) (string, error) {
	outDir, err := GetAbsolutePath(outDir)
	if err != nil {
		return "", err
	}
	return filepath.Join(outDir, TempDir), nil
}

func IsSubPath(parent, child string) (bool, error) {
	parent, err := GetAbsolutePath(parent)
	if err != nil {
		return false, err
	}
	child, err = GetAbsolutePath(child)
	if err != nil {
		return false, err
	}
	return strings.HasPrefix(child, parent), nil
}
