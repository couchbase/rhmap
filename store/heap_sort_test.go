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
	"bytes"
	"container/heap"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
)

// sortVariants are the sorting implementations that must all produce
// the same result.
var sortVariants = []struct {
	Name string
	Sort func(h *Heap, offset int64) error
}{
	{"HeapPop", (*Heap).SortByHeapPop},
	{"InPlace", (*Heap).SortInPlace},
	{"Chunked1", func(h *Heap, offset int64) error {
		return h.SortChunked(offset, 1, 0)
	}},
	{"ChunkedN", func(h *Heap, offset int64) error {
		return h.SortChunked(offset, 4, 0)
	}},
	// A tiny run size, to exercise runs that are smaller than a chunk,
	// including runs that do not divide a chunk evenly.
	{"ChunkedRuns3", func(h *Heap, offset int64) error {
		return h.SortChunked(offset, 4, 3)
	}},
}

func TestSortVariants(t *testing.T) {
	// Chunk sizes small enough that the bigger cases spill to files.
	for _, chunkSizeBytes := range []int{1 * 16, 4 * 16, 64 * 16, 1024 * 16} {
		for _, n := range []int64{0, 1, 2, 3, 10, 111, 1000} {
			for _, pattern := range dataPatterns {
				for _, offset := range []int64{0, 1, n / 2, n - 1} {
					if offset < 0 || offset > n {
						continue
					}

					for _, variant := range sortVariants {
						name := fmt.Sprintf("%s/chunk=%d/n=%d/%s/offset=%d",
							variant.Name, chunkSizeBytes, n, pattern.Name, offset)

						t.Run(name, func(t *testing.T) {
							testSortVariant(t, variant.Sort,
								chunkSizeBytes, n, pattern.Gen, offset)
						})
					}
				}
			}
		}
	}
}

func testSortVariant(t *testing.T, sortFunc func(*Heap, int64) error,
	chunkSizeBytes int, n int64, gen func(int64) [][]byte, offset int64) {
	h, cleanup := testSortHeap(t, chunkSizeBytes, chunkSizeBytes*4)
	defer cleanup()

	items := gen(n)

	for _, item := range items {
		heap.Push(h, item)
	}

	if int64(h.Len()) != n {
		t.Fatalf("h.Len() %d != %d", h.Len(), n)
	}

	err := sortFunc(h, offset)
	if err != nil {
		t.Fatalf("sort err: %v", err)
	}

	if h.Err != nil {
		t.Fatalf("h.Err: %v", h.Err)
	}

	if h.CurItems != offset {
		t.Fatalf("h.CurItems %d != offset %d", h.CurItems, offset)
	}

	// The last n-offset slots must hold the n-offset smallest items in
	// descending order, so that the min item is in the last slot.
	want := make([]string, 0, len(items))
	for _, item := range items {
		want = append(want, string(item))
	}
	sort.Strings(want)

	want = want[:n-offset]

	for i := int64(0); i < n-offset; i++ {
		got, err := h.Get(n - 1 - i)
		if err != nil {
			t.Fatalf("Get(%d) err: %v", n-1-i, err)
		}

		if string(got) != want[i] {
			t.Fatalf("slot %d got %q, want %q (all: %v)",
				n-1-i, got, want[i], want)
		}
	}

	// The leading slots must still be a valid min-heap of the
	// remaining items, and popping them must continue the sequence.
	prev := ""
	if n-offset > 0 {
		prev = want[n-offset-1]
	}

	for h.Len() > 0 {
		got := string(heap.Pop(h).([]byte))

		if got < prev {
			t.Fatalf("popped %q < prev %q", got, prev)
		}

		prev = got
	}
}

// TestSortReuse checks that a heap can be sorted, refilled, and sorted
// again, which exercises the record area swapping of SortChunked().
func TestSortReuse(t *testing.T) {
	for _, variant := range sortVariants {
		t.Run(variant.Name, func(t *testing.T) {
			h, cleanup := testSortHeap(t, 8*16, 8*16*4)
			defer cleanup()

			for round := 0; round < 3; round++ {
				n := int64(100 + round)

				for _, item := range patternRandom(n) {
					heap.Push(h, item)
				}

				err := variant.Sort(h, 0)
				if err != nil {
					t.Fatalf("round %d sort err: %v", round, err)
				}

				prev := ""

				for i := n - 1; i >= 0; i-- {
					got, err := h.Get(i)
					if err != nil {
						t.Fatalf("round %d Get(%d) err: %v", round, i, err)
					}

					if string(got) < prev {
						t.Fatalf("round %d slot %d: %q < prev %q",
							round, i, got, prev)
					}

					prev = string(got)
				}

				h.Reset()
			}
		})
	}
}

// TestSortAfterPops sorts a heap that has had items popped off of it,
// so that the heap slots hold more records than there are live items.
func TestSortAfterPops(t *testing.T) {
	for _, variant := range sortVariants {
		t.Run(variant.Name, func(t *testing.T) {
			h, cleanup := testSortHeap(t, 8*16, 8*16*4)
			defer cleanup()

			n, popped := int64(200), int64(50)

			items := patternRandom(n)

			for _, item := range items {
				heap.Push(h, item)
			}

			for i := int64(0); i < popped; i++ {
				heap.Pop(h)
			}

			if h.CurItems != n-popped {
				t.Fatalf("h.CurItems %d != %d", h.CurItems, n-popped)
			}

			if h.MaxItems != n {
				t.Fatalf("h.MaxItems %d != %d", h.MaxItems, n)
			}

			err := variant.Sort(h, 0)
			if err != nil {
				t.Fatalf("sort err: %v", err)
			}

			// The popped items were the smallest ones, so the live
			// items are the rest.
			want := make([]string, 0, len(items))
			for _, item := range items {
				want = append(want, string(item))
			}
			sort.Strings(want)

			want = want[popped:]

			for i, wantItem := range want {
				slot := n - popped - 1 - int64(i)

				got, err := h.Get(slot)
				if err != nil {
					t.Fatalf("Get(%d) err: %v", slot, err)
				}

				if string(got) != wantItem {
					t.Fatalf("slot %d got %q, want %q",
						slot, got, wantItem)
				}
			}
		})
	}
}

// TestSortThenPush pushes more items onto a heap that was just sorted,
// which reuses the heap slots that the sort left behind.
func TestSortThenPush(t *testing.T) {
	for _, variant := range sortVariants {
		t.Run(variant.Name, func(t *testing.T) {
			h, cleanup := testSortHeap(t, 8*16, 8*16*4)
			defer cleanup()

			for _, item := range patternRandom(100) {
				heap.Push(h, item)
			}

			err := variant.Sort(h, 0)
			if err != nil {
				t.Fatalf("sort err: %v", err)
			}

			// The sorted items are abandoned, and the heap is filled
			// again from scratch.
			min := ""

			for i, item := range patternAscending(20) {
				heap.Push(h, item)

				if i == 0 {
					min = string(item)
				}

				top, err := h.Get(0)
				if err != nil {
					t.Fatalf("Get(0) err: %v", err)
				}

				if string(top) != min {
					t.Fatalf("top %q != min %q", top, min)
				}
			}

			if h.Len() != 20 {
				t.Fatalf("h.Len() %d != 20", h.Len())
			}

			err = variant.Sort(h, 0)
			if err != nil {
				t.Fatalf("second sort err: %v", err)
			}

			for i := int64(0); i < 20; i++ {
				got, err := h.Get(19 - i)
				if err != nil {
					t.Fatalf("Get(%d) err: %v", 19-i, err)
				}

				if string(got) != string(patternItem(i)) {
					t.Fatalf("slot %d got %q, want %q",
						19-i, got, patternItem(i))
				}
			}
		})
	}
}

// ------------------------------------------------------

func testSortHeap(t testing.TB, heapChunkSizeBytes,
	dataChunkSizeBytes int) (*Heap, func()) {
	dir := t.TempDir()

	h := &Heap{
		LessFunc: func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		},
		Heap: &Chunks{
			PathPrefix:     filepath.Join(dir, "h"),
			FileSuffix:     ".heap",
			ChunkSizeBytes: heapChunkSizeBytes,
		},
		Data: &Chunks{
			PathPrefix:     filepath.Join(dir, "h"),
			FileSuffix:     ".data",
			ChunkSizeBytes: dataChunkSizeBytes,
		},
	}

	return h, func() { h.Close() }
}

// ------------------------------------------------------

// dataPatterns are the item orderings and cardinalities that sorting
// implementations can be sensitive to.
var dataPatterns = []struct {
	Name string
	Gen  func(n int64) [][]byte
}{
	{"random", patternRandom},
	{"ascending", patternAscending},
	{"descending", patternDescending},
	{"fewDistinct", patternFewDistinct},
	{"identical", patternIdentical},
}

const patternItemLen = 16

func patternItem(v int64) []byte {
	return []byte(fmt.Sprintf("%0*d", patternItemLen, v))
}

func patternRandom(n int64) [][]byte {
	return patternRandomLen(patternItemLen)(n)
}

// patternRandomLen returns a generator of random items of the given
// length, where the ordering is decided by the leading
// patternItemLen bytes and the rest is padding, so that the item
// length only changes how much data the items take up.
func patternRandomLen(itemLen int) func(n int64) [][]byte {
	return func(n int64) [][]byte {
		r := rand.New(rand.NewSource(1))

		pad := bytes.Repeat([]byte{'x'}, itemLen-patternItemLen)

		rv := make([][]byte, 0, n)
		for i := int64(0); i < n; i++ {
			rv = append(rv,
				append(patternItem(r.Int63n(1000000000)), pad...))
		}

		return rv
	}
}

func patternAscending(n int64) [][]byte {
	rv := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		rv = append(rv, patternItem(i))
	}

	return rv
}

func patternDescending(n int64) [][]byte {
	rv := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		rv = append(rv, patternItem(n-i))
	}

	return rv
}

func patternFewDistinct(n int64) [][]byte {
	r := rand.New(rand.NewSource(1))

	rv := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		rv = append(rv, patternItem(r.Int63n(8)))
	}

	return rv
}

func patternIdentical(n int64) [][]byte {
	rv := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		rv = append(rv, patternItem(42))
	}

	return rv
}
