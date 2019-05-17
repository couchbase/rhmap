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

// Package heap provides a min-heap data structure of []byte items
// that can automatically spill out to temporary files when the heap
// becomes large.
package heap

import (
	"encoding/binary"

	"github.com/couchbase/rhmap/store"
)

// OffsetSize associates an offset with a size.
type OffsetSize struct {
	Offset, Size uint64
}

// BytesLessFunc returns true when a is less than b.
type BytesLessFunc func(a, b []byte) bool

// Heap provides a min-heap using a given BytesLessFunc. When the
// min-heap grows too large, it will automatically spill data to
// temporary, mmap()'ed files based on the features from
// rhmap/store/Chunks. The implementation is not concurrent safe.
type Heap struct {
	// LessFunc is used to compare two data items.
	LessFunc BytesLessFunc

	// CurItems holds the # of current, live items on the heap.
	CurItems int

	// MaxItems holds the maximum # of items the heap has ever held.
	MaxItems int

	// Heap is a min-heap of offset (uint64) and size (uint64) pairs,
	// which point into the Data, and which are min-heap ordered based
	// on the LessFunc. The store.Chunks of the Heap must be
	// configured with a ChunksSizeBytes that's a multiple of 16.
	Heap *store.Chunks

	// Data represents the application data items held in chunks,
	// where each item is prefixed by its length as a uint64.
	Data *store.Chunks

	// Free represents unused but reusable slices in the Data.
	Free []OffsetSize

	// Temp is used during mutations.
	Temp []byte

	// Err tracks the first error encountered.
	Err error
}

// Error records the first error encountered.
func (h *Heap) Error(err error) error {
	if h.Err == nil {
		h.Err = err
	}
	return err
}

// Get returns the i'th item from the min-heap.
func (h *Heap) Get(i int) ([]byte, error) {
	rv, _, _, err := h.GetOffsetSize(i)
	return rv, err
}

// Get returns the i'th item from the min-heap, along with its holding
// area offset and holding area size in the data chunks.
func (h *Heap) GetOffsetSize(i int) ([]byte, uint64, uint64, error) {
	b, err := h.Heap.BytesRead(uint64(i * 16), 16)
	if err != nil {
		return nil, 0, 0, h.Error(err)
	}

	offset := binary.LittleEndian.Uint64(b[:8])
	size := binary.LittleEndian.Uint64(b[8:])

	b, err = h.Data.BytesRead(offset, size)
	if err != nil {
		return nil, 0, 0, h.Error(err)
	}

	itemLen := binary.LittleEndian.Uint64(b[:8])

	return b[8:8+itemLen], offset, size, nil
}

func (h *Heap) Len() int { return h.CurItems }

func (h *Heap) Swap(i, j int) {
	ibuf, err := h.Heap.BytesRead(uint64(i*16), 16)
	if err != nil {
		h.Error(err)
		return
	}

	jbuf, err := h.Heap.BytesRead(uint64(j*16), 16)
	if err != nil {
		h.Error(err)
		return
	}

	var buf [16]byte

	copy(buf[:], ibuf)
	copy(ibuf, jbuf)
	copy(jbuf, buf[:])
}

func (h *Heap) Less(i, j int) bool {
	iv, err := h.Get(i)
	if err != nil {
		return false
	}

	jv, err := h.Get(j)
	if err != nil {
		return false
	}

	return h.LessFunc(iv, jv)
}

// Push will error if the incoming "data length + 8" is greater than
// the configured ChunkSizeBytes of the heap's data chunks.
func (h *Heap) Push(x interface{}) {
	var buf [16]byte

	// Prepend prefix of uint64 length.
	xbytes := x.([]byte)

	binary.LittleEndian.PutUint64(buf[:8], uint64(len(xbytes)))

	h.Temp = append(h.Temp[:0], buf[:8]...)
	h.Temp = append(h.Temp, xbytes...)

	// Try to find a recycled entry from the free list.
	var offset, size uint64
	var found bool
	var err error

	for i, offsetSize := range h.Free {
		if offsetSize.Size >= uint64(len(h.Temp)) {
			offset, size = offsetSize.Offset, offsetSize.Size
			found = true

			h.Free[i] = h.Free[len(h.Free)-1]
			h.Free = h.Free[:len(h.Free)-1]

			break
		}
	}

	// Copy or append the data.
	var b []byte

	if found {
		b, err = h.Data.BytesRead(offset, size)
		if err == nil {
			copy(b, h.Temp)
		}
	} else {
		offset, size, err = h.Data.BytesAppend(h.Temp)
	}

	// Push the item's offset+size into the heap.
	if err == nil {
		if h.CurItems < h.MaxItems {
			b, err = h.Heap.BytesRead(uint64(h.CurItems * 16), 16)
			if err == nil {
				binary.LittleEndian.PutUint64(b[:8], offset)
				binary.LittleEndian.PutUint64(b[8:], size)
			}
		} else {
			binary.LittleEndian.PutUint64(buf[:8], offset)
			binary.LittleEndian.PutUint64(buf[8:], size)

			_, _, err = h.Heap.BytesAppend(buf[:])
		}
	}

	if err != nil {
		h.Error(err)
		return
	}

	h.CurItems++

	if h.MaxItems < h.CurItems {
		h.MaxItems = h.CurItems
	}
}

func (h *Heap) Pop() interface{} {
	rv, offset, size, err := h.GetOffsetSize(h.CurItems - 1)
	if err != nil {
		h.Error(err)
		return nil
	}

	h.CurItems--

	// NOTE: We immediately recycle data space used by rv, and for
	// this to work, the application is expected to copy rv if it
	// needs to hold onto that data before the next mutation.
	h.Free = append(h.Free, OffsetSize{offset, size})

	return rv
}
