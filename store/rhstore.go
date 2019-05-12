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

// Package store provides a map[[]byte][]byte based on the robin-hood
// hashmap algorithm that's hookable with persistance or storage.
package store

import (
	"bytes"
	"errors"
	"hash/fnv"
)

// ErrNilKey means a key was nil.
var ErrNilKey = errors.New("nil key")

// ErrKeyTooBig means a key was too large.
var ErrKeyTooBig = errors.New("key too big")

// ErrValTooBig means a val was too large.
var ErrValTooBig = errors.New("val too big")

// Key is the type for a key. A key with len() of 0 is invalid.
// Key max length is 2^25-1 (~33MB).
type Key []byte

// Val is the type for a val. A nil val is valid.
// Val max length is 2^25-1 (~33MB).
type Val []byte

// -------------------------------------------------------------------

// RHStore is a persisted hashmap that uses the robinhood
// algorithm. This implementation is not concurrent safe.
//
// Unlike with an RHMap, the key/val bytes placed into an RHStore are
// owned or managed by the RHStore. The RHStore's internal data
// structures are also more "flat" than an RHMap's, allowing for
// easier persistence with an RHStore.
//
// RHStore has more hook-points or callback options than an RHMap,
// which are intended for advanced users who might use the hook-points
// to build a persistent data structure.
type RHStore struct {
	// Slots are the slots of the hashmap.
	Slots []uint64

	// Size is the max number of items this hashmap can hold.
	// Size * 3 == len(Slots).
	Size int

	// Bytes is the backing slice for keys, vals and other data.
	Bytes []byte

	// Number of items in the RHStore.
	Count int

	// Overridable hash func. Defaults to hash/fnv.New32a().
	HashFunc func(Key) uint32

	// When any item's distance gets too large, grow the RHStore.
	// Defaults to 10.
	MaxDistance int

	// Overridable func to calculate a size multiplier when resizing
	// for growth is needed. Default returns a constant 2.0.
	Growth func(*RHStore) float64

	// Overridable func to grow the RHStore.
	Grow func(m *RHStore, newSize int)

	// Overridable func to truncate the backing bytes.
	BytesTruncate func(m *RHStore, n uint64)

	// Overridable func to append data to the backing bytes.
	BytesAppend func(m *RHStore, b []byte) (offset, size uint64)

	// Overridable func to read data from the backing bytes.
	BytesRead func(m *RHStore, offset, size uint64) []byte

	// Extra is for optional data that the application wants to
	// associate with the RHStore instance.
	Extra interface{}

	// Temp is used during mutations to avoid memory allocations.
	Temp Item
}

// -------------------------------------------------------------------

// Item represents an entry in the RHStore, where each item uses 3
// contiguous slots (uint64's) for encoding...
//
//           MSB....................................................LSB
// uint64 0: [64-bits keyOffset                                       ]
// uint64 1: [64-bits valOffset                                       ]
// uint64 2: [14-bits distance] | [25 bits valSize] | [25 bits keySize]
//
// The len(Item) == 3 (i.e., 3 uint64's).  The key/val offsets are
// into the RHStore's backing bytes.
type Item []uint64

const MaxKeyLen = (2 ^ 25) - 1
const MaxValLen = (2 ^ 25) - 1

const ShiftValSize = 25
const ShiftDistance = 50

const MaskKeySize = uint64(0x0000000001FFFFFF)  // 25 bits.
const MaskValSize = uint64(0x0003FFFFFE000000)  // 25 bits << ShiftValSize.
const MaskDistance = uint64(0xFFFC000000000000) // 14 bits << ShiftDistance.

func (item Item) KeyOffsetSize() (uint64, uint64) {
	return item[0], (item[2] & MaskKeySize)
}

func (item Item) ValOffsetSize() (uint64, uint64) {
	return item[1], (item[2] & MaskValSize) >> ShiftValSize
}

func (item Item) Distance() uint64 {
	return (item[2] & MaskDistance) >> ShiftDistance
}

func (item Item) DistanceAdd(x int) {
	item[2] = (item[2] & (MaskValSize | MaskKeySize)) |
		(MaskDistance & (uint64(int(item.Distance())+x) << ShiftDistance))
}

func (item Item) Encode(
	keyOffset, keySize, valOffset, valSize, distance uint64) {
	item[0] = keyOffset
	item[1] = valOffset
	item[2] = (MaskDistance & (distance << ShiftDistance)) |
		(MaskValSize & (valSize << ShiftValSize)) |
		(MaskKeySize & keySize)
}

// -------------------------------------------------------------------

// NewRHStore returns a new RHStore.
func NewRHStore(size int) *RHStore {
	h := fnv.New32a()

	return &RHStore{
		Slots: make([]uint64, size*3),

		Size: size,

		HashFunc: func(k Key) uint32 {
			h.Reset()
			h.Write(k)
			return h.Sum32()
		},

		MaxDistance: 10,
		Growth:      func(m *RHStore) float64 { return 2.0 },
		Grow:        Grow,

		BytesTruncate: BytesTruncate,
		BytesAppend:   BytesAppend,
		BytesRead:     BytesRead,

		Temp: make(Item, 3),
	}
}

// -------------------------------------------------------------------

func (m *RHStore) Item(idx int) Item {
	pos := idx * 3
	return m.Slots[pos : pos+3]
}

func (m *RHStore) ItemKey(item Item) Key {
	offset, size := item.KeyOffsetSize()
	return m.BytesRead(m, offset, size)
}

func (m *RHStore) ItemVal(item Item) Val {
	offset, size := item.ValOffsetSize()
	return m.BytesRead(m, offset, size)
}

// -------------------------------------------------------------------

// Reset clears RHStore, where already allocated memory will be reused.
func (m *RHStore) Reset() {
	slots := m.Slots
	for i := 0; i < len(slots); i++ {
		slots[i] = 0
	}

	m.BytesTruncate(m, 0)

	m.Count = 0
}

// -------------------------------------------------------------------

// Get retrieves the val for a given key.  The returned val, if found,
// is a slice into the RHStore's backing bytes and should only be used
// within its returned len() -- don't append() to the returned val.
func (m *RHStore) Get(k Key) (v Val, found bool) {
	if len(k) == 0 {
		return Val(nil), false
	}

	idx := int(m.HashFunc(k) % uint32(m.Size))
	idxStart := idx

	for {
		e := m.Item(idx)

		itemKey := m.ItemKey(e)
		if len(itemKey) == 0 {
			return Val(nil), false
		}

		if bytes.Equal(itemKey, k) {
			return m.ItemVal(e), true
		}

		idx++
		if idx >= m.Size {
			idx = 0
		}

		if idx == idxStart { // Went all the way around.
			return Val(nil), false
		}
	}
}

// -------------------------------------------------------------------

// Set inserts or updates a key/val into the RHStore. The returned
// wasNew will be true if the mutation was on a newly seen, inserted
// key, and wasNew will be false if the mutation was an update to an
// existing key.
//
// NOTE: RHStore appends or copies the incoming key/val into its
// backing bytes. Multiple updates to the same key will continue to
// grow the backing bytes -- i.e., the backing bytes are not reused or
// recycled during a Set(). Applications may instead use CopyTo() to
// copy live key/val data to another RHStore, or mutate val bytes
// in-place.
func (m *RHStore) Set(k Key, v Val) (wasNew bool, err error) {
	if len(k) == 0 {
		return false, ErrNilKey
	}

	if len(k) > MaxKeyLen {
		return false, ErrKeyTooBig
	}

	if len(v) > MaxValLen {
		return false, ErrValTooBig
	}

	idx := int(m.HashFunc(k) % uint32(m.Size))
	idxStart := idx

	// NOTE: We BytesAppend() on v before k, as an update to an
	// existing item will clip away the unused BytesAppend(k).
	newValOffset, newValSize := m.BytesAppend(m, v)
	newKeyOffset, newKeySize := m.BytesAppend(m, k)

	incoming := m.Temp
	incoming.Encode(newKeyOffset, newKeySize, newValOffset, newValSize, 0)

	for {
		e := m.Item(idx)

		itemKey := m.ItemKey(e)
		if len(itemKey) == 0 {
			copy(e, incoming)
			m.Count++
			return true, nil
		}

		if bytes.Equal(itemKey, m.ItemKey(incoming)) {
			// NOTE: We keep the same key to allow advanced apps that
			// know that they're doing an update to avoid key alloc's.
			eKeyOffset, eKeySize := e.KeyOffsetSize()

			iValOffset, iValSize := incoming.ValOffsetSize()

			e.Encode(eKeyOffset, eKeySize, iValOffset, iValSize,
				incoming.Distance())

			// Clip off the earlier BytesAppend(k) data.
			m.BytesTruncate(m, newKeyOffset)

			return false, nil
		}

		// Swap if the incoming item is further from its best idx.
		if e.Distance() < incoming.Distance() {
			for i := range incoming {
				incoming[i], e[i] = e[i], incoming[i]
			}
		}

		// Distance is another step away from best idx.
		incoming.DistanceAdd(1)

		idx++
		if idx >= m.Size {
			idx = 0
		}

		// Grow if distances become big or we went all the way around.
		if int(incoming.Distance()) > m.MaxDistance || idx == idxStart {
			k, v = m.ItemKey(incoming), m.ItemVal(incoming)

			m.Grow(m, int(float64(m.Size)*m.Growth(m)))

			return m.Set(k, v)
		}
	}
}

// -------------------------------------------------------------------

// Del removes a key/val from the RHStore. The previous val, if it
// existed, is returned.
//
// NOTE: RHStore does not remove key/val data from its backing bytes,
// so deletes of items will not reduce memory usage. Applications may
// instead use CopyTo() to copy any remaining live key/val data to
// another RHStore.
func (m *RHStore) Del(k Key) (prev Val, existed bool) {
	if len(k) == 0 {
		return Val(nil), false
	}

	idx := int(m.HashFunc(k) % uint32(m.Size))
	idxStart := idx

	for {
		e := m.Item(idx)

		itemKey := m.ItemKey(e)
		if len(itemKey) == 0 {
			return Val(nil), false
		}

		if bytes.Equal(itemKey, k) {
			prev = m.ItemVal(e)
			break // Found the item.
		}

		idx++
		if idx >= m.Size {
			idx = 0
		}

		if idx == idxStart {
			return Val(nil), false
		}
	}

	// Left-shift succeeding items in the linear chain.
	for {
		next := idx + 1
		if next >= m.Size {
			next = 0
		}

		if next == idx { // Went all the way around.
			break
		}

		maybeShift := m.Item(next)
		if len(m.ItemKey(maybeShift)) == 0 || maybeShift.Distance() <= 0 {
			break // The next item is non-shiftable.
		}

		maybeShift.DistanceAdd(-1) // Left-shift means distance drops by 1.

		copy(m.Item(idx), maybeShift)

		idx = next
	}

	item := m.Item(idx)
	for i := range item {
		item[i] = 0
	}

	m.Count--

	return prev, true
}

// -------------------------------------------------------------------

// CopyTo copies key/val's to the dest RHStore.
func (m *RHStore) CopyTo(dest *RHStore) {
	m.Visit(func(k Key, v Val) bool { dest.Set(k, v); return true })
}

// -------------------------------------------------------------------

// Visit invokes the callback on key/val. The callback can return
// false to exit the visitation early.
func (m *RHStore) Visit(callback func(k Key, v Val) (keepGoing bool)) {
	for i := 0; i < m.Size; i++ {
		e := m.Item(i)
		itemKey := m.ItemKey(e)
		if len(itemKey) != 0 {
			if !callback(itemKey, m.ItemVal(e)) {
				return
			}
		}
	}
}

// -------------------------------------------------------------------

// Grow is the default implementation to grow a RHStore.
func Grow(m *RHStore, newSize int) {
	grow := NewRHStore(newSize)
	grow.HashFunc = m.HashFunc
	grow.MaxDistance = m.MaxDistance
	grow.Growth = m.Growth
	grow.Grow = m.Grow
	grow.BytesTruncate = m.BytesTruncate
	grow.BytesAppend = m.BytesAppend
	grow.BytesRead = m.BytesRead
	grow.Extra = m.Extra

	m.CopyTo(grow)

	*m = *grow
}

// -------------------------------------------------------------------

// BytesTruncate is the default implementation to truncate the
// backing bytes of an RHStore to a given length.
func BytesTruncate(m *RHStore, size uint64) {
	m.Bytes = m.Bytes[0:size]
}

// BytesAppend is the default implementation to append data to the
// backing bytes of an RHStore.
func BytesAppend(m *RHStore, b []byte) (offset, size uint64) {
	offset = uint64(len(m.Bytes))
	m.Bytes = append(m.Bytes, b...)
	return offset, uint64(len(b))
}

// BytesRead is the default implementation to read a bytes from the
// backing bytes of an RHStore.
func BytesRead(m *RHStore, offset, size uint64) []byte {
	return m.Bytes[offset : offset+size]
}
