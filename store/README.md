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

## Chunks

Chunks represents an "append-only" sequence of persisted chunk files,
where each chunk file has the same physical size (e.g., 4MB).
