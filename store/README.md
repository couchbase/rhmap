# store - persisted data structures

The data structures provided by the store package are intended to
avoid memory allocations whenever possible, at the potential tradeoff
in ease-of-use of the API's.

When memory usage grows too large, these data structures are designed
to spill to mmap()'ed files.

These data structures are intended for ephemeral data processing,
where data spilled to files, for example, are considered
temporary. Long term persistence concerns like versioning, atomic
writes, crash recovery, etc, are not part of these implementations.

## RHStore

RHStore is a persisted hashmap that uses the robinhood algorithm.

Unlike an rhmap.RHMap, the key/val bytes placed into an RHStore are
owned or managed by the RHStore.

## Heap

Heap is a min-heap that can spill out to files, which works in
conjunction with golang's container/heap package. It can be useful for
sorting and "OFFSET/LIMIT" processing.

The heap keeps its items in two separate Chunks: a record area of
fixed-size, 16 byte offset/size records, and a data area that holds the
application's item bytes. Only the records move while sorting, so the
item bytes are never copied.

### Sorting

Heap.Sort(offset) leaves the last n-offset items of the heap slots in
sorted order, where n is the number of items in the heap and where the
min item ends up in the last slot. The leading offset slots are left as
a valid min-heap of the items that were not sorted, so an offset of 0
sorts everything, while an offset of n-k answers "the k smallest
items", which is the "ORDER BY ... LIMIT k" case.

There are three implementations in heap_sort.go, all of which leave
that same layout behind...

| implementation | extra space | in memory | spilled |
| --- | --- | --- | --- |
| SortByHeapPop | none | 1470 ms | 1500 ms |
| SortInPlace, which Sort() uses | none | 458 ms | 651 ms |
| SortChunked, 1 worker | ~16 bytes per item | 374 ms | 396 ms |
| SortChunked, 12 workers | ~16 bytes per item | 183 ms | 177 ms |

The timings are medians of 5 runs of 10 sorts of 1,000,000 random 16
byte items, on a 12 core darwin/arm64 with go1.21, where "in memory"
holds all the records in the 0'th, in-memory-only chunk, and where
"spilled" uses 64KB record chunks (245 files) and 1MB data chunks (24
files). See the BenchmarkSortXxxx benchmarks in heap_sort_bench_test.go,
which also cover other item counts, item sizes, item orderings, and
cardinalities.

  * SortByHeapPop is the original implementation, which repeatedly pops
    the min item off of the heap and places it at the end of the heap
    slots. Its ~2*n*log2(n) comparisons all chase random offsets into
    the data area, and going through container/heap costs an allocation
    per item, as Heap.Pop() returns an interface{}.

  * SortInPlace instead runs golang's sort.Sort() over the records,
    which needs no additional space and no allocations, and which
    adapts to input that is already ordered or that has few distinct
    values -- sorting 1,000,000 already ascending items takes 12 ms
    instead of 940 ms.

  * SortChunked splits the records into runs, sorts each run
    independently -- optionally with worker goroutines -- and then
    merges the sorted runs into a scratch record area, which is
    swapped in as the heap's record area. The scratch area is retained
    for reuse and is itself spillable, so the cost is ~16 bytes per
    item of scratch space rather than memory that has to be resident.

Notes on choosing between them...

  * Only a full sort benefits. When only k items are wanted, the
    heap-pop extraction does work proportional to k, so SortInPlace()
    and SortChunked() both hand small k's back to it. Measured
    crossover is around k = n/10 -- at n = 1,000,000, extracting the
    100 smallest items takes 265 us and the 10,000 smallest takes 15
    ms, against 170 ms for any full sort.

  * A full sort does not need the min-heap invariant at all, so
    callers that never pop can skip heapifying, which is another 26 ms
    per 1,000,000 items.

  * The run size matters more than the chunk size. A run never spans
    more than one record chunk, but it can be smaller, and a heap that
    has not spilled is otherwise a single run with no concurrency at
    all. Sorting many small runs is faster than sorting one big one,
    while merging many runs is slower, and the optimum in between is a
    broad one -- for 1,000,000 in-memory items and 12 workers, a single
    run took 459 ms, runs of 256 records took 273 ms, and runs of 4096
    to 65536 records all landed within run-to-run variance of each
    other, at 141 ms to 176 ms. SortRunSizeRecords() aims at the middle
    of that plateau.

  * Goroutines pay off well before they stop helping. The run sorting
    scales close to linearly -- 274 ms to 32 ms with 12 workers -- but
    the merge is sequential, so the total saturates at around 8
    workers. Small heaps are a single run, where no goroutines are
    started at all.

  * SortChunked's merge pass is unconditional, so on input that is
    already ordered, or that has few distinct values, plain
    SortInPlace() is faster.

  * SortChunked calls LessFunc from all of its workers at once, so its
    concurrency is opt-in rather than what Sort() does by default.

  * Bigger items do not change the picture. Going from 16 to 256 byte
    items at 1,000,000 spilled items costs every implementation about a
    third more time, leaving the same ratios between them.

The merge is now where the time goes: 180 ms of SortChunked's 211 ms at
1,000,000 spilled items. Ideas not yet tried, in rough order of
expected payoff: partitioning the merge by splitter items so that it
can run concurrently too; a loser tree instead of a binary heap of
cursors, which halves the merge's comparisons; letting runs span chunks
so that a spilled heap can have fewer, larger runs; and avoiding the
two integer divisions that Chunks.BytesRead() does on every item
dereference.

## Chunks

Chunks represents an "append-only" sequence of persisted chunk files,
where each chunk file has the same physical size (e.g., 4MB).
