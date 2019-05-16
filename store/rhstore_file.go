//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package store

import (
	"fmt"
	"math"
)

// CreateRHStoreFile starts a brand new RHStoreFile, which is a
// hashmap based on the robin-hood algorithm, and which will also
// spill out to mmap()'ed files if the hashmap becomes too big. The
// returned RHStoreFile is not concurrent safe. Providing a pathPrefix
// that's already in-use has undefined behavior.
func CreateRHStoreFile(pathPrefix string, options RHStoreFileOptions) (
	rv *RHStoreFile, err error) {
	sf := &RHStoreFile{
		PathPrefix: pathPrefix,
		Options:    options,
		RHStore:    *(NewRHStore(0)),
		Chunks: Chunks{
			PathPrefix:     pathPrefix,
			FileSuffix:     options.FileSuffix,
			ChunkSizeBytes: options.ChunkSizeBytes,
		},
	}

	slots, err := CreateFileAsMMapRef("", options.StartSize*8*ItemLen)
	if err != nil {
		return nil, err
	}

	sf.Slots = slots

	sf.RHStore.Slots, err = ByteSliceToUint64Slice(slots.Buf)
	if err != nil {
		return nil, err
	}

	sf.RHStore.Size = options.StartSize

	sf.RHStore.MaxDistance = options.MaxDistance

	sf.RHStore.Grow = func(m *RHStore, newSize int) error {
		return sf.Grow(newSize)
	}

	sf.RHStore.BytesTruncate = func(m *RHStore, size uint64) error {
		return sf.Chunks.BytesTruncate(size)
	}

	sf.RHStore.BytesAppend = func(m *RHStore, b []byte) (
		offsetOut, sizeOut uint64, err error) {
		return sf.Chunks.BytesAppend(b)
	}

	sf.RHStore.BytesRead = func(m *RHStore, offset, size uint64) (
		[]byte, error) {
		return sf.Chunks.BytesRead(offset, size)
	}

	return sf, nil
}

// ---------------------------------------------

// RHStoreFile represents a persisted hashmap. Its implementation is
// not concurrent safe.
//
// Unlike an RHMap or RHStore, the key's and val's in an RHStoreFile
// may not be larger than the Options.ChunkSizeBytes.
//
// The design point is to support applications that need to process or
// analyze ephemeral data which becomes large enough to not fit
// comfortably into memory, where an temporary, spillable hashmap is
// appropriate (e.g., performing a GROUP BY aggregation on CSV data
// files). A different use-case of long-term database-like storage
// (e.g., with checksums, versioning, reloading, and multithreaded
// access, etc) is not the current design target of RHStoreFile.
type RHStoreFile struct {
	// PathPrefix is the path prefix of any persisted files.
	PathPrefix string

	// Options configured for this RHStoreFile instance.
	Options RHStoreFileOptions

	// The embedded RHStore is hooked with callbacks that spill data
	// out to disk during growth, to ever larger "slots" file for item
	// metadata, and to an ever-growing sequence of "chunk" files
	// where key/val bytes are stored.
	RHStore

	// Generation is incremented whenever the hashmap metadata slots
	// are grown. See Grow(). The initial, in-memory only hashmap
	// that has the size of Options.StartSize is generation number 0.
	Generation int64

	// Slots holds the hashmap's item metadata slots. The item
	// metadata entries in the slots have offset+size uint64's that
	// reference key/val byte slices in the chunks.
	Slots *MMapRef

	// Chunks is a sequence of append-only chunk files which hold the
	// underlying key/val bytes for the hashmap.
	Chunks
}

// ---------------------------------------------

// RHStoreFileOptions represents creation-time configurable options
// for an RHStoreFile.
type RHStoreFileOptions struct {
	// Initial size of the first in-memory-only hashmap in the # of
	// items that its metadata slots can theoretically hold "fully
	// loaded" before growing.
	StartSize int

	// MaxDistance is a config on hashmap growth in that when the
	// distance of any hashmap item becomes > MaxDistance, the hashmap
	// metadata slots will be grown (and spilled to mmap()'ed files).
	MaxDistance int

	// ChunkSizeBytes is the size of each chunk file in bytes.
	// No key or val can be larger than ChunkSizeBytes.
	// ChunkSizeBytes must be > 0.
	ChunkSizeBytes int

	// FileSuffix is the file suffix used for all the files that were
	// created or managed by an RHStoreFile.
	FileSuffix string
}

// DefaultRHStoreFileOptions are the default values for options.
var DefaultRHStoreFileOptions = RHStoreFileOptions{
	StartSize:      5303,            // Ideally, a prime number.
	ChunkSizeBytes: 4 * 1024 * 1024, // 4MB.
	MaxDistance:    10,
	FileSuffix:     ".rhstore",
}

// ---------------------------------------------

// Close releases resources used by the RHStoreFile.
func (sf *RHStoreFile) Close() error {
	sf.RHStore = RHStore{}

	sf.Generation = math.MaxInt64

	if sf.Slots != nil {
		sf.Slots.Close()
		sf.Slots.Remove()
		sf.Slots = nil
	}

	sf.Chunks.Close()

	return nil
}

// ---------------------------------------------

// Grow creates a new slots file and copies over existing metadata
// items from RHStore.Slots, if any.
func (sf *RHStoreFile) Grow(nextSize int) error {
	nextGeneration := sf.Generation + 1

	nextSlotsPath := fmt.Sprintf("%s_slots_%09d%s",
		sf.PathPrefix, nextGeneration, sf.Options.FileSuffix)

	nextSlots, err :=
		CreateFileAsMMapRef(nextSlotsPath, nextSize*8*ItemLen)
	if err != nil {
		return err
	}

	cleanup := func(err error) error {
		nextSlots.Close()
		nextSlots.Remove()

		return err
	}

	var nextRHStore RHStore = sf.RHStore // Copy existing RHStore.

	nextRHStore.Slots, err = ByteSliceToUint64Slice(nextSlots.Buf)
	if err != nil {
		return cleanup(err)
	}

	nextRHStore.Size = nextSize

	nextRHStore.Count = 0

	// While copying, we temporarily max out the MaxDistance, to avoid
	// a recursion of growing while we're growing.
	origRHStoreMaxDistance := nextRHStore.MaxDistance
	nextRHStore.MaxDistance = math.MaxInt32

	// Copy the existing key/val offset/size metadata to nextRHStore.
	err = sf.RHStore.VisitOffsets(
		func(kOffset, kSize, vOffset, vSize uint64) bool {
			nextRHStore.SetOffsets(kOffset, kSize, vOffset, vSize)
			return true
		})
	if err != nil {
		return cleanup(err)
	}

	nextRHStore.MaxDistance = origRHStoreMaxDistance

	sf.RHStore = nextRHStore

	sf.Generation = nextGeneration

	if sf.Slots != nil {
		sf.Slots.Close()
		sf.Slots.Remove()
	}

	sf.Slots = nextSlots

	return nil
}
