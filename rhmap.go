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

package rhmap

import (
	"bytes"
	"errors"
	"hash/fnv"
)

var ErrNilKey = errors.New("nil key")

type Key []byte // A nil key is invalid.
type Val []byte // A nil val is valid.

// RHMap is a robinhood hashmap.  It is not concurrent safe.
type RHMap struct {
	Items []Item

	// Number of keys in the RHMap.
	Count int

	// Overridable hash func.  Defaults to hash/fnv.New32a().
	HashFunc func(Key) uint32

	// When any item's distance gets too large, grow the RHMap.
	// Defaults to 10.
	MaxDistance int

	// Overridable func to calculate a size multiplier when resizing
	// for growth is needed.  Defaults to constant 2.0.
	Growth func(*RHMap) float64

	Copy    bool // When true, RHMap copies incoming key/val's.
	CopyBuf []byte
}

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

		Growth: func(m *RHMap) float64 { return 2.0 },
	}
}

// Reset clears the RHMap (but will reuse underlying slices).
func (m *RHMap) Reset() {
	for i := range m.Items {
		m.Items[i] = Item{}
	}

	m.CopyBuf = m.CopyBuf[:0]
}

func (m *RHMap) Get(k Key) (v Val, found bool) {
	if k == nil {
		return nil, false
	}

	h := m.HashFunc(k)
	num := len(m.Items)
	idx := int(h % uint32(num))
	idxStart := idx

	for {
		e := &m.Items[idx]
		if bytes.Equal(e.Key, k) {
			return e.Val, true
		}
		if e.Key == nil {
			return nil, false
		}

		idx = idx + 1
		if idx >= num {
			idx = 0
		}

		if idx == idxStart { // Went all the way around.
			return nil, false
		}
	}
}

func (m *RHMap) Set(k Key, v Val) error {
	if k == nil {
		return ErrNilKey
	}

	h := m.HashFunc(k)
	num := len(m.Items)
	idx := int(h % uint32(num))
	idxStart := idx

	k, v = m.PrepareKeyVal(k, v)
	e := Item{k, v, 0}

	for {
		if m.Items[idx].Key == nil {
			m.Items[idx] = e
			m.Count += 1
			return nil
		}

		if bytes.Equal(m.Items[idx].Key, k) {
			m.Items[idx] = e
			return nil
		}

		// Swap if existing item is "richer" (closer to its best idx).
		if m.Items[idx].Distance < e.Distance {
			e, m.Items[idx] = m.Items[idx], e
		}

		e.Distance += 1 // One step further away from best idx.

		idx = idx + 1
		if idx >= num {
			idx = 0
		}

		// Grow if big distances or we went all the way around.
		if e.Distance > m.MaxDistance || idx == idxStart {
			grow := NewRHMap(int(float64(num) * m.Growth(m)))
			grow.HashFunc = m.HashFunc
			grow.MaxDistance = m.MaxDistance
			grow.Growth = m.Growth
			grow.Copy = m.Copy

			m.CopyTo(grow)

			*m = *grow

			return m.Set(e.Key, e.Val)
		}
	}
}

func (m *RHMap) PrepareKeyVal(k Key, v Val) (Key, Val) {
	if m.Copy {
		var n int

		n = len(m.CopyBuf)
		m.CopyBuf = append(m.CopyBuf, k...)
		k = m.CopyBuf[n:]

		n = len(m.CopyBuf)
		m.CopyBuf = append(m.CopyBuf, v...)
		v = m.CopyBuf[n:]
	}

	return k, v
}

func (m *RHMap) CopyTo(dst *RHMap) {
	m.Visit(func(k Key, v Val) bool { dst.Set(k, v); return true })
}

func (m *RHMap) Visit(cb func(k Key, v Val) (keepGoing bool)) {
	for i := range m.Items {
		e := &m.Items[i]
		if e.Key != nil {
			if !cb(e.Key, e.Val) {
				return
			}
		}
	}
}
