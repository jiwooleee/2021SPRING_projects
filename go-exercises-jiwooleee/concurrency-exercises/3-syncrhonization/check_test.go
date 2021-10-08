// DO NOT EDIT THIS PART
// Your task is to edit `main.go`

package main

import (
	"strconv"
	"testing"
)

func TestMain(t *testing.T) {
	cache := run()

	lookupLen := len(cache.lookup)
	cacheLen := cache.cache.Len()
	if lookupLen != CacheSize {
		t.Errorf("Incorrect lookup size %v", lookupLen)
	}
	if cacheLen != CacheSize {
		t.Errorf("Incorrect cache size %v", cacheLen)
	}
}

func TestLRU(t *testing.T) {
	loader := Loader{
		DB: GetMockDB(),
	}
	cache := New(&loader)

	for i := 0; i < 100; i++ {
		cache.Get("Test" + strconv.Itoa(i))
	}

	if len(cache.lookup) != 100 {
		t.Errorf("cache not 100: %d", len(cache.lookup))
	}
	cache.Get("Test0")
	cache.Get("Test101")
	if _, ok := cache.lookup["Test0"]; !ok {
		t.Errorf("0 evicted incorrectly: %v", cache.lookup)
	}

}
