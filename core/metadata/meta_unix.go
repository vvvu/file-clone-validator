package metadata

import (
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
