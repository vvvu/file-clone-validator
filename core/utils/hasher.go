package utils

import (
	"crypto/md5"
	"io"
	"os"
)

// MD5Hash returns the MD5 hash of the file at the given path.
// Input:
// - filePath: the absolute path to the file
// Output:
// - hash: the MD5 hash of the file
func MD5Hash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}
	return string(hash.Sum(nil)), nil
}
