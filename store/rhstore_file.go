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
		RHStore:    *(NewRHStore(0)),
		PathPrefix: pathPrefix,
		Options:    options,
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

	sf.MaxDistance = options.MaxDistance

	sf.Grow = func(m *RHStore, newSize int) error {
		return sf.GrowSlots(newSize)
	}

	chunkSizeBytes := options.ChunkSizeBytes

	sf.BytesTruncate = func(m *RHStore, size uint64) error {
		prevChunkLens := sf.PrevChunkLens()

		if size > uint64(prevChunkLens+chunkSizeBytes) {
			return nil
		}

		if uint64(prevChunkLens) < size {
			// The truncate is within the last chunk.
			sf.LastChunkLen = int(size) - prevChunkLens

			return nil
		}

		if size != 0 {
			return fmt.Errorf("BytesTruncate unsupported size")
		}

		// The truncate is to 0, so clear all the file-based chunks.
		for _, chunk := range sf.Chunks[1:] {
			chunk.Close() // TODO: Recycle chunk.
			chunk.Remove()
		}
		sf.Chunks = sf.Chunks[:1] // Keep 0'th in-memory-only chunk.

		sf.LastChunkLen = 0

		return nil
	}

	sf.BytesAppend = func(m *RHStore, b []byte) (
		offsetOut, sizeOut uint64, err error) {
		if len(b) > chunkSizeBytes {
			return 0, 0, fmt.Errorf(
				"BytesAppend len(b) larger than chunkSizeBytes")
		}

		if len(b) <= 0 {
			return uint64(0), uint64(0), nil
		}

		if sf.LastChunkLen+len(b) > chunkSizeBytes {
			err = sf.AddChunk(false)
			if err != nil {
				return 0, 0, err
			}
		}

		lastChunk := sf.Chunks[len(sf.Chunks)-1]

		lastChunkLen := sf.LastChunkLen

		sf.LastChunkLen = lastChunkLen + len(b)

		copy(lastChunk.Buf[lastChunkLen:sf.LastChunkLen], b)

		return uint64(sf.PrevChunkLens() + lastChunkLen), uint64(len(b)), nil
	}

	sf.BytesRead = func(m *RHStore, offset, size uint64) (
		[]byte, error) {
		if size > uint64(chunkSizeBytes) {
			return nil, fmt.Errorf(
				"BytesRead size larger than chunkSizeBytes")
		}

		chunkIdx := int(offset / uint64(chunkSizeBytes))
		if chunkIdx >= len(sf.Chunks) {
			return nil, fmt.Errorf(
				"BytesRead offset greater than chunks")
		}

		chunkOffset := offset % uint64(chunkSizeBytes)

		return sf.Chunks[chunkIdx].Buf[chunkOffset : chunkOffset+size], nil
	}

	// Add the 0'th chunk which will be in-memory-only.
	err = sf.AddChunk(true)
	if err != nil {
		sf.Close()

		return nil, err
	}

	return sf, nil
}

// ---------------------------------------------

// RHStoreFile represents a persisted hashmap. Its implementation is
// not concurrent safe.
//
// The design point is to support applications that need to process or
// analyze ephemeral data which becomes large enough to not fit
// comfortably into memory, where an temporary, spillable hashmap is
// appropriate (e.g., performing a GROUP BY aggregation on CSV data
// files). A different use-case of long-term database-like storage
// (e.g., with checksums, versioning, reloading, and multithreaded
// access, etc) is not the current design target of RHStoreFile.
type RHStoreFile struct {
	// The embedded RHStore is hooked with callbacks that spill data
	// out to disk during growth, to ever larger "slots" file for item
	// metadata, and to an ever-growing sequence of "chunk" files
	// where key/val bytes are stored.
	RHStore

	// PathPrefix is the path prefix of any persisted files.
	PathPrefix string

	// Options configured for this RHStoreFile instance.
	Options RHStoreFileOptions

	// Generation is incremented whenever the hashmap metadata slots
	// are grown. See GrowSlots(). The initial, in-memory only hashmap
	// that has the size of Options.StartSize is generation number 0.
	Generation int64

	// Slots holds the hashmap's item metadata slots. The item
	// metadata entries in the slots have offset+size uint64's that
	// reference key/val byte slices in the chunks.
	Slots *MMapRef

	// Chunks is a sequence of append-only chunk files which hold the
	// underlying key/val bytes for the hashmap. The 0'th chunk is a
	// special, in-memory-only chunk without an actual file.
	Chunks []*MMapRef

	// LastChunkLen is the logical length of the last chunk in Chunks,
	// which is the chunk that is being appended to when there are
	// new, incoming key/val items.
	LastChunkLen int

	// CloseCleanup of true means that Close() will also remove files.
	CloseCleanup bool
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

	for _, chunk := range sf.Chunks {
		chunk.Close()
		chunk.Remove()
	}
	sf.Chunks = nil

	sf.LastChunkLen = 0

	return nil
}

// ---------------------------------------------

// GrowSlots creates a new slots file and copies over existing
// metadata items from RHStore.Slots, if any.
func (sf *RHStoreFile) GrowSlots(nextSize int) error {
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

// ---------------------------------------------

// AddChunk appends a new chunk file to the RHStoreFile.
func (sf *RHStoreFile) AddChunk(memOnly bool) (err error) {
	var chunkPath string
	if !memOnly {
		chunkPath = fmt.Sprintf("%s_chunk_%09d%s",
			sf.PathPrefix, len(sf.Chunks), sf.Options.FileSuffix)
	}

	chunk, err :=
		CreateFileAsMMapRef(chunkPath, sf.Options.ChunkSizeBytes)
	if err != nil {
		return err
	}

	sf.Chunks = append(sf.Chunks, chunk)

	sf.LastChunkLen = 0

	return nil
}

// ---------------------------------------------

// PrevChunkLens returns the sum of the chunk lengths for all but the
// last chunk.
func (sf *RHStoreFile) PrevChunkLens() int {
	if len(sf.Chunks) > 1 {
		return (len(sf.Chunks) - 1) * sf.Options.ChunkSizeBytes
	}

	return 0
}
