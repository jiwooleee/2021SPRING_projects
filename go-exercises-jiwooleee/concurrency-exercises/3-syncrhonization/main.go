package main

import (
	"sync"

	"github.com/golang/glog"

	"container/list"
	"flag"
)

// Maximum size of cache.
const CacheSize = 100

// KeyStoreCacheLoader is an interface for the KeyStoreCache.
type KeyStoreCacheLoader interface {
	// Loads the value associated with the given key.
	Load(string) string
}

// A key-value pair.
type pair struct {
	Key   string
	Value string
}

// KeyStoreCache is a LRU cache for string key-value pairs.
// TODO: Make thread safe.
type KeyStoreCache struct {
	cache  list.List                // A priority queue of key-value pairs.
	lookup map[string]*list.Element // Lookup into the priority queue.
	load   func(string) string
	sync.Mutex
}

// New creates a new KeyStoreCache
func New(load KeyStoreCacheLoader) *KeyStoreCache {
	return &KeyStoreCache{
		load:   load.Load,
		lookup: make(map[string]*list.Element),
	}
}

// Get gets the key from cache, loading it if necessary.
func (k *KeyStoreCache) Get(key string) string {
	// Check if the value is in the cache.
	k.Lock()
	if e, ok := k.lookup[key]; ok {
		glog.V(1).Infof("Hit for key %v", key)
		// It becomes the most-recently-used element.
		k.cache.MoveToFront(e)
		k.Unlock()
		return e.Value.(pair).Value
	}
	glog.V(1).Infof("Miss for key %v", key)

	// Miss - load from database and save it in cache
	p := pair{
		Key:   key,
		Value: k.load(key),
	}

	// if cache is full remove the least used item
	if k.cache.Len() >= CacheSize {
		// Get least-recently-used pair, and remove it.
		end := k.cache.Back()
		k.cache.Remove(end)

		// Also remove it remove from lookup.
		delete(k.lookup, end.Value.(pair).Key)
		glog.V(1).Infof("Evicted %v", end.Value.(pair).Key)
	}

	// Put the element in the cache.
	k.cache.PushFront(p)
	// Add it to the lookup.
	k.lookup[key] = k.cache.Front()
	k.Unlock()

	return p.Value
}

// DO NOT EDIT THIS PART

// Loader implements KeyStoreLoader
type Loader struct {
	DB *MockDB
}

// Load gets the data from the database
func (l *Loader) Load(key string) string {
	val, err := l.DB.Get(key)
	if err != nil {
		panic(err)
	}

	return val
}

func run() *KeyStoreCache {
	loader := Loader{
		DB: GetMockDB(),
	}
	cache := New(&loader)

	RunMockServer(cache)

	return cache
}

func main() {
	flag.Parse()

	run()
}
