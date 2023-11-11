package metadata

import (
	"errors"
	"github.com/pkg/xattr"
	"syscall"
)

// Sys is the underlying type of the os.FileInfo.Sys() interface.
type Sys syscall.Stat_t

// toSys converts the os.FileInfo.Sys() interface to the underlying type.
// Input:
// - i: the os.FileInfo.Sys() interface
// Output:
// - *Sys: the underlying type
func toSys(i any) (*Sys, bool) {
	s, ok := i.(*syscall.Stat_t)
	if ok && s != nil {
		return (*Sys)(s), true
	}
	return nil, false
}

func (s Sys) dev() uint64   { return uint64(s.Dev) }
func (s Sys) ino() uint64   { return s.Ino }
func (s Sys) nlink() uint64 { return uint64(s.Nlink) }
func (s Sys) uid() uint32   { return s.Uid }
func (s Sys) gid() uint32   { return s.Gid }
func (s Sys) rdev() uint64  { return uint64(s.Rdev) }
func (s Sys) size() int64   { return s.Size }

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
		if e.Err == syscall.ENOTSUP || e.Err == syscall.ENOATTR {
			return nil
		}
		return err
	default:
		return err
	}
}
