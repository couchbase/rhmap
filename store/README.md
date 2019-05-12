RHStore is a hashmap that uses the robinhood algorithm.

Unlike an RHMap, the key/val bytes placed into an RHStore are owned or
managed by the RHStore.

RHStore also has more hook-points or callback API's than an RHMap, as
RHStore is intended for advanced users who might use the hook-points
to build a persistent data structure (i.e., spill data out to disk).

The RHStore's internal data structures are also more "flat" than an
RHMap's, allowing for easier persistence than when using an RHMap.
For example, the slots in an RHMap are []Item struct's, whereas the
slots in an RHStore are []uint64's.
