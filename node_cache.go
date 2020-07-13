package mast

import lru "github.com/hashicorp/golang-lru"

type NodeCache interface {
	// Add adds a freshly-persisted node to the cache.
	Add(key, value interface{})
	// Contains indicates the node with the given key has already been persisted.
	Contains(key interface{}) bool
	// Get retrieves the already-deserialized node with the given hash, if cached.
	Get(key interface{}) (value interface{}, ok bool)
}

// NewNodeCache creates a new LRU-based node cache of the given size. One cache
// can be shared by any number of trees.
func NewNodeCache(size int) NodeCache {
	cache, err := lru.NewARC(size)
	if err != nil {
		panic(err)
	}
	return cache
}
