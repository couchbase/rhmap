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

// Package rhmap provides a map[[]byte][]byte based on the robin-hood
// hashmap algorithm.
package rhmap

import (
	"bytes"
	"errors"
	"hash/fnv"
)

// ErrNilKey means a key was nil.
var ErrNilKey = errors.New("nil key")

// Key is the type for a key. A nil key is invalid.
type Key []byte

// Val is the type for a val. A nil val is valid.
type Val []byte

// RHMap is a robinhood hashmap. It is not concurrent safe.
type RHMap struct {
	Items []Item

	// Number of keys in the RHMap.
	Count int

	// Overridable hash func. Defaults to hash/fnv.New32a().
	HashFunc func(Key) uint32

	// When any item's distance gets too large, grow the RHMap.
	// Defaults to 10.
	MaxDistance int

	// Overridable func to calculate a size multiplier when resizing
	// for growth is needed. Default returns a constant 2.0.
	Growth func(*RHMap) float64

	// Overridable func to grow the RHMap.
	Grow func(m *RHMap, newSize int)
}

// Item represents an entry in the RHMap.
type Item struct {
	Key Key
	Val Val

	Distance int // How far item is from its best position.
}

// NewRHMap returns a new robinhood hashmap.
func NewRHMap(size int) *RHMap {
	h := fnv.New32a()

	return &RHMap{
		Items: make([]Item, size),

		HashFunc: func(k Key) uint32 {
			h.Reset()
			h.Write(k)
			return h.Sum32()
		},

		MaxDistance: 10,
		Growth:      func(m *RHMap) float64 { return 2.0 },
		Grow:        Grow,
	}
}

// Reset clears RHMap, where already allocated memory will be reused.
func (m *RHMap) Reset() {
	for i := range m.Items {
		m.Items[i] = Item{}
	}

	m.Count = 0
}

// Get retrieves the val for a given key.
func (m *RHMap) Get(k Key) (v Val, found bool) {
	if k == nil {
		return Val(nil), false
	}

	num := len(m.Items)
	idx := int(m.HashFunc(k) % uint32(num))
	idxStart := idx

	for {
		e := &m.Items[idx]
		if e.Key == nil {
			return Val(nil), false
		}

		if bytes.Equal(e.Key, k) {
			return e.Val, true
		}

		idx++
		if idx >= num {
			idx = 0
		}

		if idx == idxStart { // Went all the way around.
			return Val(nil), false
		}
	}
}

// Set inserts or updates a key/val into the RHMap. The returned
// wasNew will be true if the mutation was on a newly seen, inserted
// key, and wasNew will be false if the mutation was an update to an
// existing key.
//
// NOTE: RHMap does not keep its own copy of the key/val's, so the
// application should not mutate them (especially the key).
func (m *RHMap) Set(k Key, v Val) (wasNew bool, err error) {
	if k == nil {
		return false, ErrNilKey
	}

	num := len(m.Items)
	idx := int(m.HashFunc(k) % uint32(num))
	idxStart := idx

	incoming := Item{k, v, 0}

	for {
		e := &m.Items[idx]
		if e.Key == nil {
			m.Items[idx] = incoming
			m.Count++
			return true, nil
		}

		if bytes.Equal(e.Key, k) {
			m.Items[idx] = incoming
			return false, nil
		}

		// Swap if the incoming item is further from its best idx.
		if e.Distance < incoming.Distance {
			incoming, m.Items[idx] = m.Items[idx], incoming
		}

		incoming.Distance++ // One step further away from best idx.

		idx++
		if idx >= num {
			idx = 0
		}

		// Grow if distances become big or we went all the way around.
		if incoming.Distance > m.MaxDistance || idx == idxStart {
			m.Grow(m, int(float64(num)*m.Growth(m)))

			return m.Set(incoming.Key, incoming.Val)
		}
	}
}

// Del removes a key/val from the RHMap. The previous val, if it
// existed, is returned.
func (m *RHMap) Del(k Key) (prev Val, existed bool) {
	if k == nil {
		return Val(nil), false
	}

	num := len(m.Items)
	idx := int(m.HashFunc(k) % uint32(num))
	idxStart := idx

	for {
		e := &m.Items[idx]
		if e.Key == nil {
			return Val(nil), false
		}

		if bytes.Equal(e.Key, k) {
			prev = e.Val
			break // Found the item.
		}

		idx++
		if idx >= num {
			idx = 0
		}

		if idx == idxStart {
			return Val(nil), false
		}
	}

	// Left-shift succeeding items in the linear chain.
	for {
		next := idx + 1
		if next >= num {
			next = 0
		}

		if next == idx { // Went all the way around.
			break
		}

		f := &m.Items[next]
		if f.Key == nil || f.Distance <= 0 {
			break
		}

		f.Distance--

		m.Items[idx] = *f

		idx = next
	}

	m.Items[idx] = Item{}
	m.Count--

	return prev, true
}

// CopyTo copies key/val's to the dst.
func (m *RHMap) CopyTo(dst *RHMap) {
	m.Visit(func(k Key, v Val) bool { dst.Set(k, v); return true })
}

// Visit invokes the callback on key/val. The callback can return
// false to exit the visitation early.
func (m *RHMap) Visit(callback func(k Key, v Val) (keepGoing bool)) {
	for i := range m.Items {
		e := &m.Items[i]
		if e.Key != nil {
			if !callback(e.Key, e.Val) {
				return
			}
		}
	}
}

// Grow is the default implementation to grow a RHMap.
func Grow(m *RHMap, newSize int) {
	grow := NewRHMap(newSize)
	grow.HashFunc = m.HashFunc
	grow.MaxDistance = m.MaxDistance
	grow.Growth = m.Growth
	grow.Grow = m.Grow

	m.CopyTo(grow)

	*m = *grow
}
