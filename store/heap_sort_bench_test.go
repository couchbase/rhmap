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
	"container/heap"
	"fmt"
	"testing"
)

// The sorting benchmarks vary four things that the sorting
// implementations can be sensitive to...
//
//   - the number of items -- as the record area and the data grow past
//     the caches, comparisons start chasing memory instead of cache;
//   - whether the heap has spilled to files -- the "memory" layouts use
//     chunks that are large enough that everything stays in the 0'th,
//     in-memory-only chunk, and the "spilled" layouts use chunks that
//     are small enough that most of the items live in mmap()'ed files;
//   - the ordering and cardinality of the items -- see dataPatterns;
//   - how many of the sorted items the caller actually wants, which is
//     the offset param of Sort().
//
// Setup work is kept out of the timings: the items are pushed and
// heapified once, and each iteration restores the record area from a
// snapshot of the heapified records. Restoring only needs the record
// area, as sorting moves records around but never touches the item
// data. Restoring does dirty the record area's mmap()'ed pages,
// though, so some of the writeback that it provokes can land in the
// timed region that follows it.
//
// The B/op and allocs/op of the SortChunked() variants are dominated
// by the one-time creation of their scratch record area, which the
// first iteration pays for and later iterations reuse -- a steady
// state sort allocates nothing.
//
// Run, for example, with:
//
//	go test -run=NONE -bench=BenchmarkSortMemory -benchtime=5x ./store
type sortBenchLayout struct {
	Name string

	// HeapChunkSizeBytes and DataChunkSizeBytes are per item, and are
	// multiplied by the number of items to size the chunks.
	HeapChunkSizeBytes, DataChunkSizeBytes func(n int) int
}

var sortBenchMemory = sortBenchLayout{
	Name: "memory",
	// One chunk that's big enough to hold everything, so the heap
	// never spills to a file.
	HeapChunkSizeBytes: func(n int) int { return n * RecordSizeBytes },
	DataChunkSizeBytes: func(n int) int { return n * 64 },
}

var sortBenchSpilled = sortBenchLayout{
	Name: "spilled",
	// Chunks of 64KB of records (4096 records) and 1MB of data, so a
	// million items spill into ~250 record files and ~40 data files.
	HeapChunkSizeBytes: func(n int) int { return 64 * 1024 },
	DataChunkSizeBytes: func(n int) int { return 1024 * 1024 },
}

var sortBenchSizes = []int{1000, 100000, 1000000}

var sortBenchVariants = []struct {
	Name string
	Sort func(h *Heap, offset int64) error
}{
	{"HeapPop", (*Heap).SortByHeapPop},
	{"InPlace", (*Heap).SortInPlace},
	{"Chunked-1", func(h *Heap, offset int64) error {
		return h.SortChunked(offset, 1, 0)
	}},
	{"Chunked-N", func(h *Heap, offset int64) error {
		return h.SortChunked(offset, 0, 0)
	}},
	// Runs of 4096 records, which for the "memory" layout is the only
	// way to get more than the one run that its single big chunk
	// otherwise gives.
	{"Runs4096-1", func(h *Heap, offset int64) error {
		return h.SortChunked(offset, 1, 4096)
	}},
	{"Runs4096-N", func(h *Heap, offset int64) error {
		return h.SortChunked(offset, 0, 4096)
	}},
}

func BenchmarkSortMemory(b *testing.B)  { benchmarkSort(b, sortBenchMemory) }
func BenchmarkSortSpilled(b *testing.B) { benchmarkSort(b, sortBenchSpilled) }

func benchmarkSort(b *testing.B, layout sortBenchLayout) {
	for _, n := range sortBenchSizes {
		for _, pattern := range dataPatterns {
			// A full sort, then wanting only the 100 smallest items,
			// which is the "ORDER BY ... LIMIT 100" case.
			offsets := []int64{0, int64(n) - 100}

			for _, offset := range offsets {
				wanted := "all"
				if offset > 0 {
					wanted = fmt.Sprintf("%d", int64(n)-offset)
				}

				for _, variant := range sortBenchVariants {
					name := fmt.Sprintf("n=%d/%s/want=%s/%s",
						n, pattern.Name, wanted, variant.Name)

					b.Run(name, func(b *testing.B) {
						benchmarkSortVariant(b, layout,
							n, pattern.Gen, offset, variant.Sort, true)
					})
				}
			}
		}
	}
}

// BenchmarkSortHeapify measures the heapifying that a full sort does
// not actually need, for comparison against the full sort timings.
func BenchmarkSortHeapify(b *testing.B) {
	for _, layout := range []sortBenchLayout{sortBenchMemory, sortBenchSpilled} {
		for _, n := range sortBenchSizes {
			name := fmt.Sprintf("%s/n=%d/random", layout.Name, n)

			b.Run(name, func(b *testing.B) {
				benchmarkSortVariant(b, layout, n, patternRandom, 0,
					func(h *Heap, offset int64) error {
						heap.Init(h)
						return h.Err
					}, false)
			})
		}
	}
}

// BenchmarkSortRunSize sweeps the run size of SortChunked(), which
// trades the locality of each run's sort against the depth of the
// merge.
func BenchmarkSortRunSize(b *testing.B) {
	for _, layout := range []sortBenchLayout{sortBenchMemory, sortBenchSpilled} {
		for _, n := range sortBenchSizes {
			for _, runSize := range []int{256, 1024, 4096, 16384, 65536, 0} {
				for _, workers := range []int{1, 0} {
					runSizeName := fmt.Sprintf("%d", runSize)
					if runSize == 0 {
						runSizeName = "chunk"
					}

					workersName := "1"
					if workers == 0 {
						workersName = "N"
					}

					name := fmt.Sprintf("%s/n=%d/random/runSize=%s/workers=%s",
						layout.Name, n, runSizeName, workersName)

					b.Run(name, func(b *testing.B) {
						benchmarkSortVariant(b, layout, n, patternRandom, 0,
							func(h *Heap, offset int64) error {
								return h.SortChunked(offset, workers, runSize)
							}, true)
					})
				}
			}
		}
	}
}

// BenchmarkSortWorkers sweeps the worker goroutine count of
// SortChunked(), to see where the concurrent run sorting stops paying
// off against the sequential merge.
func BenchmarkSortWorkers(b *testing.B) {
	for _, layout := range []sortBenchLayout{sortBenchMemory, sortBenchSpilled} {
		for _, n := range sortBenchSizes {
			for _, workers := range []int{1, 2, 4, 8, 12} {
				name := fmt.Sprintf("%s/n=%d/random/workers=%d",
					layout.Name, n, workers)

				b.Run(name, func(b *testing.B) {
					benchmarkSortVariant(b, layout, n, patternRandom, 0,
						func(h *Heap, offset int64) error {
							return h.SortChunked(offset, workers, 4096)
						}, true)
				})
			}
		}
	}
}

// BenchmarkSortPhases splits SortChunked() into its run sorting phase
// and its merge phase, as the merge is sequential and bounds how much
// the concurrent run sorting can win.
func BenchmarkSortPhases(b *testing.B) {
	phases := []struct {
		Name string
		Sort func(h *Heap, workers, runSize int) error
	}{
		{"runsOnly", func(h *Heap, workers, runSize int) error {
			return sortRecordRuns(h.recordRuns(h.CurItems, runSize), workers)
		}},
		{"runsAndMerge", func(h *Heap, workers, runSize int) error {
			return h.SortChunked(0, workers, runSize)
		}},
	}

	for _, layout := range []sortBenchLayout{sortBenchMemory, sortBenchSpilled} {
		for _, n := range sortBenchSizes {
			for _, workers := range []int{1, 0} {
				for _, phase := range phases {
					workersName := "1"
					if workers == 0 {
						workersName = "N"
					}

					name := fmt.Sprintf("%s/n=%d/random/workers=%s/%s",
						layout.Name, n, workersName, phase.Name)

					b.Run(name, func(b *testing.B) {
						benchmarkSortVariant(b, layout, n, patternRandom, 0,
							func(h *Heap, offset int64) error {
								return phase.Sort(h, workers, 4096)
							}, phase.Name != "runsOnly")
					})
				}
			}
		}
	}
}

// BenchmarkSortLimit finds where wanting only the k smallest items
// stops being cheaper than a full sort. The heap-pop extraction does
// work proportional to k, while a full sort's cost does not depend on
// k at all -- a full, descending sort leaves the k smallest items in
// the last k slots, which answers the same question.
func BenchmarkSortLimit(b *testing.B) {
	for _, layout := range []sortBenchLayout{sortBenchMemory, sortBenchSpilled} {
		for _, n := range sortBenchSizes {
			ks := []int64{10, 100, 1000, 10000, 100000, int64(n) / 4, int64(n)}

			for _, k := range ks {
				if k > int64(n) {
					continue
				}

				name := fmt.Sprintf("%s/n=%d/random/k=%d/HeapPop",
					layout.Name, n, k)

				b.Run(name, func(b *testing.B) {
					benchmarkSortVariant(b, layout, n, patternRandom,
						int64(n)-k, (*Heap).SortByHeapPop, false)
				})
			}

			// The full sort, for comparison against every k above.
			name := fmt.Sprintf("%s/n=%d/random/k=all/Chunked-N", layout.Name, n)

			b.Run(name, func(b *testing.B) {
				benchmarkSortVariant(b, layout, n, patternRandom, 0,
					func(h *Heap, offset int64) error {
						return h.SortChunked(offset, 0, 4096)
					}, true)
			})
		}
	}
}

// BenchmarkSortItemSize varies how much data the items take up, which
// changes how far apart the comparisons have to reach.
func BenchmarkSortItemSize(b *testing.B) {
	variants := []struct {
		Name string
		Sort func(h *Heap, offset int64) error
	}{
		{"HeapPop", (*Heap).SortByHeapPop},
		{"InPlace", (*Heap).SortInPlace},
		{"Chunked-N", func(h *Heap, offset int64) error {
			return h.SortChunked(offset, 0, 4096)
		}},
	}

	for _, layout := range []sortBenchLayout{sortBenchMemory, sortBenchSpilled} {
		for _, n := range []int{100000, 1000000} {
			for _, itemLen := range []int{16, 64, 256} {
				for _, variant := range variants {
					name := fmt.Sprintf("%s/n=%d/itemLen=%d/%s",
						layout.Name, n, itemLen, variant.Name)

					b.Run(name, func(b *testing.B) {
						benchmarkSortVariant(b, layout, n,
							patternRandomLen(itemLen), 0, variant.Sort, true)
					})
				}
			}
		}
	}
}

func benchmarkSortVariant(b *testing.B, layout sortBenchLayout,
	n int, gen func(int64) [][]byte, offset int64,
	sortFunc func(*Heap, int64) error, checkSorted bool) {
	h, cleanup := testSortHeap(b,
		layout.HeapChunkSizeBytes(n), layout.DataChunkSizeBytes(n))
	defer cleanup()

	for _, item := range gen(int64(n)) {
		err := h.PushBytes(item)
		if err != nil {
			b.Fatalf("PushBytes err: %v", err)
		}
	}

	heap.Init(h)

	if h.Err != nil {
		b.Fatalf("h.Err: %v", h.Err)
	}

	// Snapshot the heapified records, so that each iteration can be
	// given the same starting arrangement.
	snapshot := make([]byte, 0, n*RecordSizeBytes)

	for i := 0; i < int(h.CurItems); i++ {
		rec, err := h.Heap.BytesRead(uint64(i*RecordSizeBytes), RecordSizeBytes)
		if err != nil {
			b.Fatalf("BytesRead err: %v", err)
		}

		snapshot = append(snapshot, rec...)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		err := h.Heap.BytesTruncate(0)
		if err != nil {
			b.Fatalf("BytesTruncate err: %v", err)
		}

		err = appendRecords(h.Heap, snapshot)
		if err != nil {
			b.Fatalf("appendRecords err: %v", err)
		}

		h.CurItems = int64(n)
		h.MaxItems = int64(n)
		h.Free = h.Free[:0]

		b.StartTimer()

		err = sortFunc(h, offset)
		if err != nil {
			b.Fatalf("sort err: %v", err)
		}
	}

	b.StopTimer()

	// A last sanity check that the benchmarked sort actually sorted.
	if checkSorted && offset == 0 && h.MaxItems > 1 {
		prev, err := h.Get(h.MaxItems - 1)
		if err != nil {
			b.Fatalf("Get err: %v", err)
		}

		prev = append([]byte(nil), prev...)

		for i := h.MaxItems - 2; i >= 0; i-- {
			cur, err := h.Get(i)
			if err != nil {
				b.Fatalf("Get err: %v", err)
			}

			if h.LessFunc(cur, prev) {
				b.Fatalf("slot %d is out of order", i)
			}

			prev = append(prev[:0], cur...)
		}
	}
}
