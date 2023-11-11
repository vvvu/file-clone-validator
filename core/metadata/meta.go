package metadata

import (
	"file-clone-validator/core/utils"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// FSType is the type of the file system.
const (
	FSTypeFile       = "file"
	FSTypeDir        = "dir"
	FSTypeSymlink    = "symlink"
	FSTypeCharDevice = "chardev"
	FSTypeDevice     = "dev"
	FSTypeNamedPipe  = "fifo"
	FSTypeSocket     = "socket"
	FSTypeUnknown    = "unknown"
)

// RetrieveFileSystemMeta retrieves the file system fbs of the file at the given path.
// Input:
// - path: the path to the file
// - fi: the os.FileInfo of the file
// Output:
// - fbs: the file system fbs of the file
func RetrieveFileSystemMeta(path string, fi os.FileInfo) (*Meta, error) {
	path, err := filepath.Abs(path) // replace the relative path with the absolute path
	if err != nil {
		return nil, err
	}

	// fill the basic fbs attributes
	mask := os.ModePerm | os.ModeType | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	meta := &Meta{
		Common: CommonAttrs{
			Path: path,      // absolute path in the source file system
			Name: fi.Name(), // file name without the directory path
		},
		FileSystem: &FileSystemAttrs{
			Mode:    fi.Mode() & mask, // file mode bits
			ModTime: uint64(fi.ModTime().Unix()),
		},
		ObjectStorage: nil, // file system fbs does not include object storage attributes
	}

	// fill the file type
	switch fi.Mode() & (os.ModeType | os.ModeCharDevice) {
	case 0:
		meta.FileSystem.Type = FSTypeFile
	case os.ModeDir:
		meta.FileSystem.Type = FSTypeDir
	case os.ModeSymlink:
		meta.FileSystem.Type = FSTypeSymlink
	case os.ModeDevice | os.ModeCharDevice:
		meta.FileSystem.Type = FSTypeCharDevice
	case os.ModeDevice:
		meta.FileSystem.Type = FSTypeDevice
	case os.ModeNamedPipe:
		meta.FileSystem.Type = FSTypeNamedPipe
	case os.ModeSocket:
		meta.FileSystem.Type = FSTypeSocket
	default:
		meta.FileSystem.Type = FSTypeUnknown
		slog.Warn("Unknown file type", "path", path, "mode", fi.Mode())
	}

	if meta.FileSystem.Type == FSTypeFile { // file-specific attributes
		meta.Common.Size = uint64(fi.Size()) // file size in bytes
		meta.Common.Hash, err = utils.MD5Hash(path)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate the md5 hash of the file %s: %w", path, err)
		}
	}

	// fill the extra underlying file system attributes
	underSys, ok := toSys(fi.Sys())
	if !ok {
		meta.FileSystem.UID = uint32(os.Getuid()) // user id of the owner
		meta.FileSystem.GID = uint32(os.Getgid()) // group id of the owner
	} else {
		switch meta.FileSystem.Type {
		case FSTypeFile:
			meta.Common.Size = uint64(underSys.size()) // file size in bytes
			meta.FileSystem.Links = underSys.nlink()   // number of hard links
		case FSTypeDir:
		case FSTypeSymlink:
			meta.FileSystem.Links = underSys.nlink() // number of hard links
			meta.FileSystem.LinkTarget, err = os.Readlink(path)
			if err != nil {
				return nil, fmt.Errorf("failed to read the link target of the symlink %s: %w", path, err)
			}
		case FSTypeDevice, FSTypeCharDevice:
			meta.FileSystem.Links = underSys.nlink() // number of hard links
		case FSTypeNamedPipe, FSTypeSocket, FSTypeUnknown:
		default:
			return meta, nil // return the fbs without the file system attributes
		}

		// retrieve the extended attributes
		xattrs, _err := ListXAttr(path)
		if _err != nil {
			return nil, fmt.Errorf("failed to list the extended attributes of the file %s: %w", path, _err)
		}

		meta.ExtendedAttributes = make([]ExtendedAttribute, 0, len(xattrs))
		for _, xattr := range xattrs {
			value, __err := GetXAttr(path, xattr)
			if __err != nil {
				slog.Warn("Failed to get the extended attribute", "path", path, "xattr", xattr, "err", __err)
				continue
			}
			meta.ExtendedAttributes = append(meta.ExtendedAttributes, ExtendedAttribute{
				Key:   xattr,
				Value: value,
			})
		}
	}

	return meta, nil
}

// RetrieveObjectStorageMeta TODO: implement this function
func RetrieveObjectStorageMeta(object string) (*Meta, error) {
	return nil, nil
}

// Meta is the main structure that combines common and source-specific attributes.
type Meta struct {
	Common        CommonAttrs
	FileSystem    *FileSystemAttrs
	ObjectStorage *ObjectStorageAttrs

	ExtendedAttributes ExtendedAttributes
}

// CommonAttrs captures attributes that are common across different storage systems.
type CommonAttrs struct {
	// Path is the full source path to the file including the file name. It can be used to
	// construct the full destination path.
	Path string

	// Name is the file name without the directory path.
	Name string

	// Size is the size of the file in bytes.
	Size uint64

	// Hash is the hash of the file calculated by the MD5 algorithm. MD5 is typically faster in generating hash values,
	// but it is less secure than SHA-256. It's considered cryptographically broken and unsuitable for further use
	// in any security-sensitive applications. It's vulnerable to hash collisions. However, it's still useful for
	// detecting accidental data corruption in current use cases.
	Hash string
}

// FileSystemAttrs captures file-system-specific attributes.
type FileSystemAttrs struct {
	// Type is the type of the file. It can be one of the following values:
	// - file: a regular file
	// - dir: a directory
	// - symlink: a symbolic link
	// - chardev: a character device
	// - dev: a block device
	// - fifo: a named pipe
	// - socket: a socket
	Type string

	// Mode is the file mode bits.
	Mode os.FileMode

	// ModTime is the modification time of the file in seconds since the Unix epoch.
	ModTime uint64

	// UID is the user id of the owner.
	UID uint32

	// GID is the group id of the owner.
	GID uint32

	// Links is the number of hard links.
	Links uint64

	// LinkTarget is the path to the target of the symbolic link.
	LinkTarget string
}

// ObjectStorageAttrs captures object-storage-specific attributes.
type ObjectStorageAttrs struct {
	// StorageClass is the storage class of the object.
	StorageClass string

	// LastModified is the last modified time of the object in seconds since the Unix epoch.
	LastModified uint64
}

// ExtendedAttribute captures the key-value pair of an extended attribute.
type ExtendedAttribute struct {
	Key   string
	Value []byte
}

type ExtendedAttributes []ExtendedAttribute
