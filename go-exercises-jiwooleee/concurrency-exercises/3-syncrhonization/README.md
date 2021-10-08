# Exercise 3: Mutual Exclusion

# A concurrent cache

In this exercise, we will use a [mutex](https://gobyexample.com/mutexes) (see
also [this exercise](https://tour.golang.org/concurrency/9)) to protect a data
structure from being modified concurrently.

Our scenario is a [cache](https://en.wikipedia.org/wiki/Cache_(computing)) for a
database of [key-value
pairs](https://en.wikipedia.org/wiki/Attribute%E2%80%93value_pair) (you can
think of this sort of database as essentially a large map from keys to values).

In `main.go`, the `KeyStoreCache` is the cache data structure. It contains a
priority queue of the 100 most recently accessed items from the key store
(implemented as a linked list), along with a map to quickly lookup elements from
the linked list.

This data structure is accessed by clients via the `Get` function to look up
entries from the cache. If the element is present in the cache, it is returned
directly. If it is not, the cache loads the element from the database and puts
it in the cache. 

The cache has a maximum size so it uses a bounded amount of memory. When the
cache is full and a new element is loaded, the least-recently-accessed element
from the cache is _evicted_.

## Hints

* You should use a mutex to guard access to the internals of the cache from
  multiple concurrent clients.
* A correct solution is only a few lines of code.

*Note*: It is possible to make this more performant by taking advantage of the
fact that concurrent _reads_ of the map are ok (see [this
answer](https://groups.google.com/g/golang-nuts/c/HpLWnGTp-n8/m/hyUYmnWJqiQJ) by
Rob Pike), while a write may be concurrent with no access. This optimization is
left as extra credit. You may use `sync.Map` if you want, or use finer-grained
locking.

## Test your solution

Use the following command to test for race conditions and correct functionality:
```
go test -race
```

Incorrect solutions generate output, since data races are found by the Go race
detector.

Correct solution:
```
go test -race
PASS
ok      concurrency-exercises/3-syncrhonization 4.619s
```

Incorrect solution:
```
==================
WARNING: DATA RACE
Write at 0x00c0001527a8 by goroutine 15:
...
```

### Related real-world caches (in Go)

* [https://www.mailgun.com/blog/golangs-superior-cache-solution-memcached-redis/](https://www.mailgun.com/blog/golangs-superior-cache-solution-memcached-redis/)
* [https://allegro.tech/2016/03/writing-fast-cache-service-in-go.html](https://allegro.tech/2016/03/writing-fast-cache-service-in-go.html)
