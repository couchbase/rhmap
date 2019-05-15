//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package store

import (
	"fmt"
	"os"

	"github.com/edsrzf/mmap-go"
)

// MMapPageSize is the default page size in bytes used by rhstore.
var MMapPageSize = int64(4096)

// CreateFileAsMMapRef creates a new, empty file of the given size in
// bytes and mmap()'s it.  If the path is "", then an in-memory-only
// MMapRef is returned, which is an MMapRef that really isn't
// mmap()'ing an actual file.
func CreateFileAsMMapRef(path string, size int) (*MMapRef, error) {
	if path == "" {
		return &MMapRef{Buf: make([]byte, size), Refs: 1}, nil
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Write 0 at the last byte to grow its size.
	_, err = file.WriteAt([]byte{0}, int64(size-1))
	if err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}

	mmapRef, err := MMapFileRegion(path, file, 0, int64(size), true)
	if err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}

	return mmapRef, err
}

// ----------------------------------------------------------

// MMapRef provides a ref-counting wrapper around a mmap handle. The
// implementation is not concurrent safe.
type MMapRef struct {
	Path string
	File *os.File
	MMap mmap.MMap
	Buf  []byte
	Refs int
}

func (r *MMapRef) AddRef() *MMapRef {
	if r == nil {
		return nil
	}

	r.Refs++

	return r
}

func (r *MMapRef) DecRef() error {
	if r == nil {
		return nil
	}

	r.Refs--
	if r.Refs <= 0 {
		r.Buf = nil

		if r.MMap != nil {
			r.MMap.Unmap()
			r.MMap = nil
		}

		if r.File != nil {
			r.File.Close()
			r.File = nil
		}
	}

	return nil
}

// Close is an alias for DecRef(), allowing MMapRef to implement the
// io.Closer interface.
func (r *MMapRef) Close() error { return r.DecRef() }

// ----------------------------------------------------------

// Remove should be called only on a closed MMapRef.
func (r *MMapRef) Remove() error {
	if r.Path != "" {
		return os.Remove(r.Path)
	}

	return nil
}

// ----------------------------------------------------------

// MMapFileRegion mmap()'s a region of bytes in an os.File.
func MMapFileRegion(path string, file *os.File, offset, size int64,
	readWrite bool) (*MMapRef, error) {
	// Some platforms (windows) only support mmap()'ing at a
	// granularity != to pageSize, so calculate a compatible
	// offset/size to use.
	offsetActual := pageOffset(offset, MMapPageGranularity)
	offsetDelta := int64(offset - offsetActual)
	sizeActual := size + offsetDelta

	// Check whether the file meets the required size.
	fstats, err := file.Stat()
	if err != nil || sizeActual > int64(fstats.Size()) {
		return nil, fmt.Errorf("mmap: file.Name: %s, err: %+v",
			fstats.Name(), err)
	}

	mode := mmap.RDONLY
	if readWrite {
		mode = mmap.RDWR
	}

	mmap, err := mmap.MapRegion(file, int(sizeActual), mode, 0, offsetActual)
	if err != nil {
		return nil, fmt.Errorf("mmap: offsetActual: %v, sizeActual: %v, "+
			"file.Name: %s, file.Mode: %v, file.ModTime: %v, err: %v",
			offsetActual, sizeActual,
			fstats.Name(), fstats.Mode(), fstats.ModTime(), err)
	}

	buf := mmap[offsetDelta : offsetDelta+size]

	return &MMapRef{Path: path, File: file, MMap: mmap, Buf: buf, Refs: 1}, nil
}

// ---------------------------------------------

// pageOffset returns the page offset for a given pos.
func pageOffset(pos, pageSize int64) int64 {
	rem := pos % pageSize
	if rem != 0 {
		return pos - rem
	}
	return pos
}
