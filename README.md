# rhmap - a robin-hood hashmap in golang

In other words: `map[[]byte][]byte`

## Example
```go
    var size = 97 // Ideally, some prime number.

    m := rhmap.NewRHMap(size)

    wasNew, err := m.Set([]byte("hi"), []byte("world"))
    // wasNew == true

    v, ok := m.Get([]byte("hi"))
    // ok == true
    // v == []byte("world")

    previous, existed := m.Del([]byte("hi")
    // existed == true
    // previous == []byte("world")
```

## Some features

* Visit() method with key-val callback.
* CopyTo(anotherRHMap) method.
* Overridable hash function.
* Overridable growth multiplier function.
* Automatic growth when linear probe distances become larger than the
  MaxDistance configuration.
* Ability to copy incoming Key & Val bytes -- see the Copy flag.
* Reset() method allows an RHMap to be efficiently cleared, and the
  underlying, already allocated memory will be recycled for reuse,
  which can reduce garbage memory pressure for some applications.

## Motivations

The RHMap was intended for a use case where many application data
objects needed to be processed, where the processing of a single data
object needed its own temporary hashmap instance.  The standard golang
hashmap did not support []byte keys, so conversions from []byte
to-and-from strings (i.e., we were using `map[string][]byte`) was
creating garbage.  Instead, we needed a (mythical) map[[]byte][]byte.

## License

Apache 2.0
