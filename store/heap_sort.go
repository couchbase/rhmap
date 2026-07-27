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
	"encoding/binary"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"container/heap"
)

// RecordSizeBytes is the size of a heap record, which is an offset
// (uint64) and size (uint64) pair that refers into the Data chunks.
const RecordSizeBytes = 16

// ------------------------------------------------------

// SortByHeapPop pops items off the heap and places them at the end of
// the heap slots in reverse order, leaving sorted items at the end of
// the heap slots. This approach does not allocate additional space. If
// there are n items in the heap, then n-offset items will be left
// sorted at the end of the heap slots. An offset of 0 sorts the entire
// heap.
//
// This is the original, "brute force" heap-sort: it costs roughly
// 2*(n-offset)*log2(n) comparisons, where each comparison chases two
// random offsets into the Data chunks. Its strength is a small,
// bounded amount of work when only a few items are wanted (e.g., a
// small LIMIT), and it leaves the leading offset slots as a still
// valid min-heap.
func (h *Heap) SortByHeapPop(offset int64) error {
	for i := h.CurItems - 1; i >= offset; i-- {
		_, offset, size, err := h.GetOffsetSize(0)
		if err != nil {
			return h.Error(err)
		}

		heap.Pop(h)
		if h.Err != nil {
			return h.Err
		}

		h.Free = h.Free[:0]

		err = h.SetOffsetSize(i, offset, size)
		if err != nil {
			return h.Error(err)
		}
	}

	return nil
}

// ------------------------------------------------------

// SortInPlace sorts the heap's records in-place, leaving the same
// layout that SortByHeapPop() leaves behind, but using an introsort
// (golang's sort.Sort) over the fixed-size records instead of
// repeatedly popping the min-heap. It does not allocate additional
// space.
//
// A full sort (offset of 0) does not need the min-heap invariant, so
// callers that only ever want a full sort can skip heapifying their
// items altogether.
//
// When offset is greater than 0, only n-offset items are wanted, and
// SortByHeapPop() is used instead, as it both does less work and
// leaves the leading offset slots as a valid min-heap.
func (h *Heap) SortInPlace(offset int64) error {
	if offset > 0 {
		return h.SortByHeapPop(offset)
	}

	n := h.CurItems
	if n > 1 {
		if runs := h.recordRuns(n, 0); len(runs) == 1 {
			// Fast path when all the records are in one chunk, which
			// avoids the offset-to-chunk math on every access.
			sort.Sort(&runs[0])

			if runs[0].Err != nil {
				return h.Error(runs[0].Err)
			}
		} else {
			hr := &heapRun{Heap: h, Beg: 0, End: n}

			sort.Sort(hr)

			if hr.Err != nil {
				return h.Error(hr.Err)
			}
		}
	}

	h.CurItems = 0

	// The data of every item is still referenced by the sorted
	// records, so nothing is reusable.
	h.Free = h.Free[:0]

	return h.Err
}

// ------------------------------------------------------

// SortChunked sorts the heap's records by splitting them into runs,
// sorting each run independently, and then merging the sorted runs
// into a scratch record area, which is finally swapped in as the
// heap's record area. The result has the same layout that
// SortByHeapPop() leaves behind.
//
// Sorting each run independently keeps the working set of each sort
// small, and the runs can be sorted concurrently by up to the given
// number of worker goroutines -- a workers of 0 means GOMAXPROCS, and
// a workers of 1 means no goroutines are used. The merge phase is
// sequential, and reads and writes the record areas linearly.
//
// A run never spans more than one record chunk, and runSizeRecords
// further limits a run to that many records -- a runSizeRecords of 0
// means a run per record chunk. Smaller runs sort faster, as their
// records and their referenced data are more likely to stay in the
// caches, but more runs make for a deeper merge.
//
// Unlike SortByHeapPop() and SortInPlace(), this needs scratch space
// for the merge output of roughly 16 bytes per item, which is itself a
// spillable Chunks. The scratch space is retained for reuse by later
// sorts, and is released by Close().
//
// The heap's LessFunc must be safe for concurrent use when workers is
// not 1.
//
// When offset is greater than 0, only n-offset items are wanted, and
// SortByHeapPop() is used instead. See SortInPlace().
func (h *Heap) SortChunked(offset int64, workers, runSizeRecords int) error {
	if offset > 0 {
		return h.SortByHeapPop(offset)
	}

	n := h.CurItems
	if n > 1 {
		runs := h.recordRuns(n, runSizeRecords)

		err := sortRecordRuns(runs, workers)
		if err != nil {
			return h.Error(err)
		}

		if len(runs) > 1 {
			err = h.mergeRecordRuns(runs)
			if err != nil {
				return h.Error(err)
			}
		}
	}

	h.CurItems = 0

	h.Free = h.Free[:0]

	return h.Err
}

// minSortRunSizeRecords is the smallest run size that
// SortRunSizeRecords() will suggest, as smaller runs sort no faster
// once a run's records and their data fit in the caches, while they do
// make the merge deeper.
const minSortRunSizeRecords = 4096

// SortRunSizeRecords suggests a runSizeRecords for SortChunked(),
// aiming for enough runs to keep the given number of workers busy --
// a workers of 0 means GOMAXPROCS -- without splitting the items into
// so many runs that the sequential merge becomes needlessly deep. It
// must be called before sorting, as it goes by the heap's current
// number of items.
func (h *Heap) SortRunSizeRecords(workers int) int {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	// A couple of runs per worker, which is enough that a worker that
	// draws an expensive run does not hold up the merge, while keeping
	// the merge shallow -- fewer, larger runs measured better than
	// more, smaller ones.
	runSize := h.CurItems / int64(2*workers)

	if runSize < minSortRunSizeRecords {
		runSize = minSortRunSizeRecords
	}

	return int(runSize)
}

// ------------------------------------------------------

// dataItem returns the application data item that's referenced by the
// leading record of the given buffer. Unlike Heap.Get(), any error is
// returned to the caller instead of being recorded in the heap, so
// that dataItem() is usable from concurrent goroutines.
func (h *Heap) dataItem(rec []byte) ([]byte, error) {
	offset := binary.LittleEndian.Uint64(rec[:8])
	size := binary.LittleEndian.Uint64(rec[8:16])

	b, err := h.Data.BytesRead(offset, size)
	if err != nil {
		return nil, err
	}

	itemLen := binary.LittleEndian.Uint64(b[:8])

	return b[8 : 8+itemLen], nil
}

// swapRecords swaps the two records at the given byte positions of the
// same buffer.
func swapRecords(buf []byte, i, j int) {
	var tmp [RecordSizeBytes]byte

	ibuf := buf[i : i+RecordSizeBytes]
	jbuf := buf[j : j+RecordSizeBytes]

	copy(tmp[:], ibuf)
	copy(ibuf, jbuf)
	copy(jbuf, tmp[:])
}

// ------------------------------------------------------

// recordRun implements sort.Interface over a run of records that live
// contiguously in a single chunk. The ordering is descending by data
// item, which matches the layout that Heap.Sort() leaves behind, where
// the min item ends up in the last slot.
type recordRun struct {
	Heap *Heap

	// Buf holds the run's records, and its length is a multiple of
	// RecordSizeBytes.
	Buf []byte

	// Err tracks the first error encountered while comparing.
	Err error
}

func (r *recordRun) Len() int { return len(r.Buf) / RecordSizeBytes }

func (r *recordRun) Less(i, j int) bool {
	iv, err := r.Heap.dataItem(r.Buf[i*RecordSizeBytes:])
	if err != nil {
		if r.Err == nil {
			r.Err = err
		}
		return false
	}

	jv, err := r.Heap.dataItem(r.Buf[j*RecordSizeBytes:])
	if err != nil {
		if r.Err == nil {
			r.Err = err
		}
		return false
	}

	return r.Heap.LessFunc(jv, iv) // Descending.
}

func (r *recordRun) Swap(i, j int) {
	swapRecords(r.Buf, i*RecordSizeBytes, j*RecordSizeBytes)
}

// ------------------------------------------------------

// heapRun implements sort.Interface over a range of heap slots that
// may span multiple chunks, ordered descending by data item.
type heapRun struct {
	Heap *Heap

	Beg, End int64

	Err error
}

func (r *heapRun) Len() int { return int(r.End - r.Beg) }

func (r *heapRun) Less(i, j int) bool {
	iv, err := r.Heap.Get(r.Beg + int64(i))
	if err != nil {
		if r.Err == nil {
			r.Err = err
		}
		return false
	}

	jv, err := r.Heap.Get(r.Beg + int64(j))
	if err != nil {
		if r.Err == nil {
			r.Err = err
		}
		return false
	}

	return r.Heap.LessFunc(jv, iv) // Descending.
}

func (r *heapRun) Swap(i, j int) {
	r.Heap.Swap(int(r.Beg)+i, int(r.Beg)+j)
}

// ------------------------------------------------------

// recordRuns splits the first n records of the heap slots into runs of
// at most runSizeRecords records, where a run never spans more than
// one record chunk, and a runSizeRecords of 0 means a run per chunk.
func (h *Heap) recordRuns(n int64, runSizeRecords int) []recordRun {
	recsPerChunk := int64(h.Heap.ChunkSizeBytes / RecordSizeBytes)

	runSize := int64(runSizeRecords)
	if runSize <= 0 || runSize > recsPerChunk {
		runSize = recsPerChunk
	}

	h.sortRuns = h.sortRuns[:0]

	for i := 0; i < len(h.Heap.Chunks); i++ {
		chunkBeg := int64(i) * recsPerChunk
		if chunkBeg >= n {
			break
		}

		chunkEnd := chunkBeg + recsPerChunk
		if chunkEnd > n {
			chunkEnd = n
		}

		buf := h.Heap.Chunks[i].Buf

		for beg := chunkBeg; beg < chunkEnd; beg += runSize {
			end := beg + runSize
			if end > chunkEnd {
				end = chunkEnd
			}

			h.sortRuns = append(h.sortRuns, recordRun{
				Heap: h,
				Buf: buf[(beg-chunkBeg)*RecordSizeBytes : (end-chunkBeg)*
					RecordSizeBytes],
			})
		}
	}

	return h.sortRuns
}

// sortRecordRuns sorts each run, optionally using worker goroutines,
// where a workers of 0 means GOMAXPROCS and a workers of 1 means no
// goroutines are used.
func sortRecordRuns(runs []recordRun, workers int) error {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	if workers > len(runs) {
		workers = len(runs)
	}

	if workers <= 1 {
		for i := range runs {
			sort.Sort(&runs[i])

			if runs[i].Err != nil {
				return runs[i].Err
			}
		}

		return nil
	}

	// The workers take the next unsorted run until the runs are done,
	// as the runs can differ widely in their cost to sort.
	next := int64(-1)

	var wg sync.WaitGroup

	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()

			for {
				i := int(atomic.AddInt64(&next, 1))
				if i >= len(runs) {
					return
				}

				sort.Sort(&runs[i])
			}
		}()
	}

	wg.Wait()

	for i := range runs {
		if runs[i].Err != nil {
			return runs[i].Err
		}
	}

	return nil
}

// ------------------------------------------------------

// mergeCursor tracks the unconsumed records of a sorted run, along
// with the run's current head data item.
type mergeCursor struct {
	Buf  []byte
	Item []byte
}

// mergeRecordRuns merges the given sorted runs into the heap's scratch
// record area, and swaps that scratch area in as the heap's record
// area.
func (h *Heap) mergeRecordRuns(runs []recordRun) error {
	if h.sortScratch == nil {
		h.sortScratch = &Chunks{
			PathPrefix:     h.Heap.PathPrefix,
			FileSuffix:     h.Heap.FileSuffix + "_sort",
			ChunkSizeBytes: h.Heap.ChunkSizeBytes,
		}
	}

	scratch := h.sortScratch

	err := scratch.BytesTruncate(0)
	if err != nil {
		return err
	}

	err = growFirstChunk(scratch, h.CurItems*RecordSizeBytes)
	if err != nil {
		return err
	}

	// Load the cursors and heapify them, largest head item first.
	cursors := h.sortCursors[:0]

	for i := range runs {
		if len(runs[i].Buf) <= 0 {
			continue
		}

		item, err := h.dataItem(runs[i].Buf)
		if err != nil {
			return err
		}

		cursors = append(cursors, mergeCursor{Buf: runs[i].Buf, Item: item})
	}

	h.sortCursors = cursors

	for i := len(cursors)/2 - 1; i >= 0; i-- {
		h.siftDownCursors(cursors, i)
	}

	// Repeatedly append the largest head record to the scratch area.
	if cap(h.sortBuf) < mergeBufSizeBytes {
		h.sortBuf = make([]byte, 0, mergeBufSizeBytes)
	}

	out := h.sortBuf[:0]

	for len(cursors) > 0 {
		top := &cursors[0]

		out = append(out, top.Buf[:RecordSizeBytes]...)

		if len(out) >= mergeBufSizeBytes {
			err = appendRecords(scratch, out)
			if err != nil {
				return err
			}

			out = out[:0]
		}

		top.Buf = top.Buf[RecordSizeBytes:]

		if len(top.Buf) <= 0 {
			cursors[0] = cursors[len(cursors)-1]
			cursors = cursors[:len(cursors)-1]
		} else {
			top.Item, err = h.dataItem(top.Buf)
			if err != nil {
				return err
			}
		}

		h.siftDownCursors(cursors, 0)
	}

	h.sortBuf = out

	err = appendRecords(scratch, out)
	if err != nil {
		return err
	}

	// Swap the merged scratch area in as the heap's record area.
	h.Heap, h.sortScratch = scratch, h.Heap

	// The merged record area holds exactly the merged records, so
	// there are no longer any leftover slots past them.
	h.MaxItems = h.CurItems

	return nil
}

// mergeBufSizeBytes is the amount of merged output that's batched up
// before being appended to the scratch record area, and must be a
// multiple of RecordSizeBytes.
const mergeBufSizeBytes = 8 * 1024

// siftDownCursors restores the cursor heap, which is ordered with the
// largest head item first.
func (h *Heap) siftDownCursors(cursors []mergeCursor, i int) {
	for {
		child := i*2 + 1
		if child >= len(cursors) {
			return
		}

		if right := child + 1; right < len(cursors) &&
			h.LessFunc(cursors[child].Item, cursors[right].Item) {
			child = right
		}

		if !h.LessFunc(cursors[i].Item, cursors[child].Item) {
			return
		}

		cursors[i], cursors[child] = cursors[child], cursors[i]

		i = child
	}
}

// ------------------------------------------------------

// growFirstChunk grows the capacity of the 0'th chunk of the chunks,
// which is the in-memory-only chunk that otherwise grows by append(),
// so that filling it does not have to reallocate it over and over. The
// chunks must be empty.
func growFirstChunk(cs *Chunks, sizeBytes int64) error {
	if len(cs.Chunks) <= 0 {
		err := cs.AddChunk()
		if err != nil {
			return err
		}
	}

	if sizeBytes > int64(cs.ChunkSizeBytes) {
		sizeBytes = int64(cs.ChunkSizeBytes)
	}

	chunk := cs.Chunks[0]

	if chunk.File == nil && int64(cap(chunk.Buf)) < sizeBytes {
		chunk.Buf = make([]byte, 0, sizeBytes)
	}

	return nil
}

// appendRecords appends records to chunks, where the given bytes are a
// multiple of RecordSizeBytes and the chunks have a ChunkSizeBytes
// that's a multiple of RecordSizeBytes. Records must exactly fill each
// chunk, as a partly filled chunk would leave a gap in the offset
// space, which would break the record index to offset mapping used by
// Heap.GetOffsetSize().
func appendRecords(cs *Chunks, b []byte) error {
	for len(b) > 0 {
		avail := 0
		if len(cs.Chunks) > 0 {
			avail = cs.ChunkSizeBytes - cs.LastChunkLen
		}

		if avail <= 0 {
			err := cs.AddChunk()
			if err != nil {
				return err
			}

			avail = cs.ChunkSizeBytes
		}

		n := len(b)
		if n > avail {
			n = avail
		}

		_, _, err := cs.BytesAppend(b[:n])
		if err != nil {
			return err
		}

		b = b[n:]
	}

	return nil
}
