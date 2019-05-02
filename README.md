# rhmap - a robin-hood hashmap in golang

## Example
```
    var size = SomePrimeNumber

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

## Advanced features

* Grows automatically when linear probe distances grow bigger than the
  MaxDistance configuration.
* Recycle the RHMap for reuse via the Reset() method, which
  reuses already allocated memory structures.
* Ability to copy incoming Key & Val bytes.  See the Copy flag.
* Overridable the hashing function.
* Overridable growth multiplier function.

## License

Apache 2.0
