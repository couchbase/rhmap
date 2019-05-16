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
)

// Chunks tracks a sequence of persisted chunk files.
type Chunks struct {
	PathPrefix, FileSuffix string

	// ChunkSizeBytes is the size of each chunk file.
	ChunkSizeBytes int

	// Chunks is a sequence of append-only chunk files. An example
	// usage is to hold the underlying key/val bytes for a
	// hashmap. The 0'th chunk is a special, in-memory-only chunk
	// without an actual backing file.
	Chunks []*MMapRef

	// LastChunkLen is the logical length of the last chunk, which is
	// the chunk that is still being appended to when there are new,
	// incoming data items.
	LastChunkLen int
}

// ---------------------------------------------

func (cs *Chunks) BytesTruncate(size uint64) error {
	prevChunkLens := cs.PrevChunkLens()

	if size > uint64(prevChunkLens+cs.ChunkSizeBytes) {
		return nil
	}

	if uint64(prevChunkLens) < size {
		// The truncate is within the last chunk.
		cs.LastChunkLen = int(size) - prevChunkLens

		if len(cs.Chunks) == 1 {
			// Special case the 0'th in-memory chunk.
			cs.Chunks[0].Buf = cs.Chunks[0].Buf[:cs.LastChunkLen]
		}

		return nil
	}

	if size != 0 {
		return fmt.Errorf("chunk: BytesTruncate unsupported size")
	}

	// The truncate is to 0, so clear all the file-based chunks.
	for _, chunk := range cs.Chunks[1:] {
		chunk.Close() // TODO: Recycle chunk.
		chunk.Remove()
	}
	cs.Chunks = cs.Chunks[:1] // Keep 0'th in-memory-only chunk.

	// Special case the 0'th in-memory chunk.
	cs.Chunks[0].Buf = cs.Chunks[0].Buf[:0]

	cs.LastChunkLen = 0

	return nil
}

// ---------------------------------------------

func (cs *Chunks) BytesAppend(b []byte) (
	offsetOut, sizeOut uint64, err error) {
	if len(b) > cs.ChunkSizeBytes {
		return 0, 0, fmt.Errorf(
			"chunk: BytesAppend len(b) > ChunkSizeBytes")
	}

	if len(b) <= 0 {
		return 0, 0, nil
	}

	if len(cs.Chunks) <= 0 || cs.LastChunkLen+len(b) > cs.ChunkSizeBytes {
		err = cs.AddChunk()
		if err != nil {
			return 0, 0, err
		}
	}

	lastChunk := cs.Chunks[len(cs.Chunks)-1]

	lastChunkLen := cs.LastChunkLen

	cs.LastChunkLen = lastChunkLen + len(b)

	// Special case in-memory only chunk which uses append().
	if lastChunk.File == nil {
		lastChunk.Buf = append(lastChunk.Buf, b...)
	} else {
		copy(lastChunk.Buf[lastChunkLen:cs.LastChunkLen], b)
	}

	return uint64(cs.PrevChunkLens() + lastChunkLen), uint64(len(b)), nil
}

// ---------------------------------------------

func (cs *Chunks) BytesRead(offset, size uint64) (
	[]byte, error) {
	if size > uint64(cs.ChunkSizeBytes) {
		return nil, fmt.Errorf(
			"chunk: BytesRead size > ChunkSizeBytes")
	}

	chunkIdx := int(offset / uint64(cs.ChunkSizeBytes))
	if chunkIdx >= len(cs.Chunks) {
		return nil, fmt.Errorf(
			"chunk: BytesRead offset greater than chunks")
	}

	chunkOffset := offset % uint64(cs.ChunkSizeBytes)

	return cs.Chunks[chunkIdx].Buf[chunkOffset : chunkOffset+size], nil
}

// ---------------------------------------------

// Close releases resources used by the chunk files.
func (cs *Chunks) Close() error {
	for _, chunk := range cs.Chunks {
		chunk.Close()
		chunk.Remove()
	}
	cs.Chunks = nil

	cs.LastChunkLen = 0

	return nil
}

// ---------------------------------------------

// AddChunk appends a new chunk file to the chunks.
func (cs *Chunks) AddChunk() (err error) {
	var chunkPath string
	var chunkSizeBytes int

	if len(cs.Chunks) > 0 {
		chunkPath = fmt.Sprintf("%s_chunk_%09d%s",
			cs.PathPrefix, len(cs.Chunks), cs.FileSuffix)
		chunkSizeBytes = cs.ChunkSizeBytes
	}

	chunk, err := CreateFileAsMMapRef(chunkPath, chunkSizeBytes)
	if err != nil {
		return err
	}

	cs.Chunks = append(cs.Chunks, chunk)

	cs.LastChunkLen = 0

	return nil
}

// ---------------------------------------------

// PrevChunkLens returns the sum of the chunk lengths for all but the
// last chunk.
func (cs *Chunks) PrevChunkLens() int {
	if len(cs.Chunks) > 1 {
		return (len(cs.Chunks) - 1) * cs.ChunkSizeBytes
	}

	return 0
}
