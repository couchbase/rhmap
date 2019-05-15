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

// ErrKeyZeroLen means a key was nil.
var ErrKeyZeroLen = errors.New("key zero len")

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

// RHStore is a hashmap that uses the robinhood algorithm. This
// implementation is not concurrent safe.
//
// Unlike an RHMap, the key/val bytes placed into an RHStore are
// copied and owned by the RHStore. The RHStore's internal data
// structures are also more "flat" than an RHMap's, allowing for
// easier persistence with an RHStore.
//
// RHStore has more hook-points or callback options than an RHMap,
// which are intended for advanced users who might use the hook-points
// to build a persistent data structure.
type RHStore struct {
	// Slots are the backing data for item metadata of the hashmap.
	// 3 slots are used to represent a single item's metadata.
	Slots []uint64

	// Size is the max number of items this hashmap can hold.
	// Size * 3 == len(Slots).
	Size int

	// Bytes is the backing slice for key/val data that's used by the
	// default BytesTruncate/Append/Read() implementations.
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
	Grow func(m *RHStore, newSize int) error

	// Overridable func to truncate the backing bytes.
	BytesTruncate func(m *RHStore, n uint64) error

	// Overridable func to append data to the backing bytes.
	BytesAppend func(m *RHStore, b []byte) (offset, size uint64, err error)

	// Overridable func to read data from the backing bytes.
	BytesRead func(m *RHStore, offset, size uint64) ([]byte, error)

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

const ItemLen = 3 // Number of uint64's needed for item metadata.

// MaxKeyLen is representable by 25 bit number, or ~33MB.
const MaxKeyLen = (1 << 25) - 1

// MaxKeyLen is representable by 25 bit number, or ~33MB.
const MaxValLen = (1 << 25) - 1

const ShiftValSize = 25  // # of bits to left-shift a 25-bit ValSize.
const ShiftDistance = 50 // # of bits to left-shift a 14-bit Distance.

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

// NewRHStore returns a ready-to-use RHStore.
func NewRHStore(size int) *RHStore {
	h := fnv.New32a()

	return &RHStore{
		Slots: make([]uint64, size*ItemLen),

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

		Temp: make(Item, ItemLen),
	}
}

// -------------------------------------------------------------------

func (m *RHStore) Item(idx int) Item {
	pos := idx * ItemLen
	return m.Slots[pos : pos+ItemLen]
}

func (m *RHStore) ItemKey(item Item) (Key, error) {
	offset, size := item.KeyOffsetSize()
	return m.BytesRead(m, offset, size)
}

func (m *RHStore) ItemVal(item Item) (Val, error) {
	offset, size := item.ValOffsetSize()
	return m.BytesRead(m, offset, size)
}

// -------------------------------------------------------------------

// Reset clears RHStore, where already allocated memory will be reused.
func (m *RHStore) Reset() error {
	slots := m.Slots
	for i := 0; i < len(slots); i++ {
		slots[i] = 0
	}

	m.Count = 0

	return m.BytesTruncate(m, 0)
}

// -------------------------------------------------------------------

// Get retrieves the val for a given key. The returned val, if found,
// is a slice into the RHStore's backing bytes and should only be used
// within its returned len() -- don't append() to the returned val as
// that might incorrectly overwrite unrelated data.
func (m *RHStore) Get(k Key) (v Val, found bool) {
	if len(k) == 0 {
		return Val(nil), false
	}

	idx := int(m.HashFunc(k) % uint32(m.Size))
	idxStart := idx

	for {
		e := m.Item(idx)

		itemKey, err := m.ItemKey(e)
		if err != nil || len(itemKey) == 0 {
			return Val(nil), false
		}

		if bytes.Equal(itemKey, k) {
			itemVal, err := m.ItemVal(e)
			if err != nil {
				return Val(nil), false
			}

			return itemVal, true
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
// wasNew will be true if the mutation was on a newly-seen inserted
// key, and wasNew will be false if the mutation was an update to an
// existing key.
//
// NOTE: RHStore appends or copies the incoming key/val into its
// backing bytes. Multiple updates to the same key will continue to
// grow the backing bytes -- i.e., the backing bytes are not reused or
// recycled during a Set(). Applications that need to really remove
// deleted bytes may instead use CopyTo() to copy live key/val data to
// another RHStore. Applications might also mutate val bytes in-place
// as another way to save allocations.
func (m *RHStore) Set(k Key, v Val) (wasNew bool, err error) {
	if len(k) == 0 {
		return false, ErrKeyZeroLen
	}

	if len(k) > MaxKeyLen {
		return false, ErrKeyTooBig
	}

	if len(v) > MaxValLen {
		return false, ErrValTooBig
	}

	// NOTE: BytesAppend() on v before k since an update to an
	// existing item will clip away the unused BytesAppend(k).
	vOffset, vSize, err := m.BytesAppend(m, v)
	if err != nil {
		return false, err
	}

	kOffset, kSize, err := m.BytesAppend(m, k)
	if err != nil {
		return false, err
	}

	wasNew, err = m.SetOffsets(kOffset, kSize, vOffset, vSize)
	if err == nil && wasNew == false {
		// Truncate off the earlier BytesAppend(k) since updates will
		// reuse the existing key.
		err = m.BytesTruncate(m, kOffset)
	}

	return wasNew, err
}

func (m *RHStore) SetOffsets(kOffset, kSize, vOffset, vSize uint64) (
	wasNew bool, err error) {
	incoming := m.Temp
	incoming.Encode(kOffset, kSize, vOffset, vSize, 0)

	incomingItemKey, err := m.ItemKey(incoming)
	if err != nil {
		return false, err
	}

	idx := int(m.HashFunc(incomingItemKey) % uint32(m.Size))
	idxStart := idx

	for {
		e := m.Item(idx)

		itemKey, err := m.ItemKey(e)
		if err != nil {
			return false, err
		}

		if len(itemKey) == 0 {
			copy(e, incoming)
			m.Count++
			return true, nil
		}

		itemKeyIncoming, err := m.ItemKey(incoming)
		if err != nil {
			return false, err
		}

		if bytes.Equal(itemKey, itemKeyIncoming) {
			// NOTE: We keep the same key during an update to avoid
			// a duplicate key allocation.
			eKeyOffset, eKeySize := e.KeyOffsetSize()

			iValOffset, iValSize := incoming.ValOffsetSize()

			e.Encode(eKeyOffset, eKeySize, iValOffset, iValSize,
				incoming.Distance())

			return false, nil
		}

		// Swap if the incoming item is further from its best idx,
		// which is the robin-hood algorithm's main headline.
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
			k, err := m.ItemKey(incoming)
			if err != nil {
				return false, err
			}

			v, err := m.ItemVal(incoming)
			if err != nil {
				return false, err
			}

			kCopy := append([]byte(nil), k...)
			vCopy := append([]byte(nil), v...)

			err = m.Grow(m, int(float64(m.Size)*m.Growth(m)))
			if err != nil {
				return false, err
			}

			return m.Set(kCopy, vCopy)
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
// another, potentially smaller RHStore.
func (m *RHStore) Del(k Key) (prev Val, existed bool, err error) {
	if len(k) == 0 {
		return Val(nil), false, ErrKeyZeroLen
	}

	idx := int(m.HashFunc(k) % uint32(m.Size))
	idxStart := idx

	for {
		e := m.Item(idx)

		itemKey, err := m.ItemKey(e)
		if err != nil || len(itemKey) == 0 {
			return Val(nil), false, err
		}

		if bytes.Equal(itemKey, k) {
			prev, err = m.ItemVal(e)
			if err != nil {
				return Val(nil), false, err
			}

			break // Found the item.
		}

		idx++
		if idx >= m.Size {
			idx = 0
		}

		if idx == idxStart {
			return Val(nil), false, nil
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

		maybeShiftKey, err := m.ItemKey(maybeShift)
		if err != nil {
			return Val(nil), false, err
		}

		if len(maybeShiftKey) == 0 || maybeShift.Distance() <= 0 {
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

	return prev, true, nil
}

// -------------------------------------------------------------------

// CopyTo copies key/val's to the dest RHStore.
func (m *RHStore) CopyTo(dest *RHStore) {
	m.Visit(func(k Key, v Val) bool { dest.Set(k, v); return true })
}

// -------------------------------------------------------------------

// Visit invokes the callback on key/val. The callback can return
// false to stop the visitation early.
func (m *RHStore) Visit(
	callback func(k Key, v Val) (keepGoing bool)) error {
	for i := 0; i < m.Size; i++ {
		e := m.Item(i)

		itemKey, err := m.ItemKey(e)
		if err != nil {
			return err
		}

		if len(itemKey) != 0 {
			itemVal, err := m.ItemVal(e)
			if err != nil {
				return err
			}

			if !callback(itemKey, itemVal) {
				return nil
			}
		}
	}

	return nil
}

// VisitOffsets invokes the callback on each item's key/val
// offsets. The callback can return false to stop the visitation
// early.
func (m *RHStore) VisitOffsets(
	callback func(kOffset, kSize, vOffset, vSize uint64) (keepGoing bool)) error {
	for i := 0; i < m.Size; i++ {
		e := m.Item(i)

		kOffset, kSize := e.KeyOffsetSize()
		if kOffset != 0 && kSize != 0 {
			vOffset, vSize := e.ValOffsetSize()

			if !callback(kOffset, kSize, vOffset, vSize) {
				return nil
			}
		}
	}

	return nil
}

// -------------------------------------------------------------------

// Grow is the default implementation to grow a RHStore.
func Grow(m *RHStore, newSize int) error {
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

	return nil
}

// -------------------------------------------------------------------

// BytesTruncate is the default implementation to truncate the
// backing bytes of an RHStore to a given length.
func BytesTruncate(m *RHStore, size uint64) error {
	m.Bytes = m.Bytes[0:size]
	return nil
}

// BytesAppend is the default implementation to append data to the
// backing bytes of an RHStore.
func BytesAppend(m *RHStore, b []byte) (offset, size uint64, err error) {
	offset = uint64(len(m.Bytes))
	m.Bytes = append(m.Bytes, b...)
	return offset, uint64(len(b)), nil
}

// BytesRead is the default implementation to read a bytes from the
// backing bytes of an RHStore.
func BytesRead(m *RHStore, offset, size uint64) ([]byte, error) {
	return m.Bytes[offset : offset+size], nil
}
