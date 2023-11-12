//go:build darwin || freebsd || linux || netbsd || openbsd
// +build darwin freebsd linux netbsd openbsd

package metadata

import (
	"errors"
	"github.com/pkg/xattr"
	"syscall"
)

// GetXAttr retrieves extended attribute data associated with path.
func GetXAttr(path, name string) ([]byte, error) {
	b, err := xattr.LGet(path, name)
	return b, handleXAttrErr(err)
}

// ListXAttr retrieves a list of names of extended attributes associated with the given path.
func ListXAttr(path string) ([]string, error) {
	l, err := xattr.LList(path)
	return l, handleXAttrErr(err)
}

// handleXAttrErr handles the error returned by the xattr package.
// Input:
// - err: the error returned by the xattr package
func handleXAttrErr(err error) error {
	if err == nil {
		return nil
	}
	var e *xattr.Error
	switch {
	case errors.As(err, &e):
		if e.Err == syscall.ENOTSUP || e.Err == xattr.ENOATTR {
			return nil
		}
		return err
	default:
		return err
	}
}
