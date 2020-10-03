package mast

import (
	"context"
	"fmt"
	"sync"
)

type inMemoryStore struct {
	entries map[string][]byte
	l       sync.Mutex
}

// NewInMemoryStore provides a Persist that stores serialized nodes in a map, usually for testing.
func NewInMemoryStore() Persist {
	return &inMemoryStore{}
}

func (ims *inMemoryStore) Store(ctx context.Context, key string, value []byte) error {
	// fmt.Printf("ims %p setting %s...\n", ims, key)
	ims.l.Lock()
	if ims.entries == nil {
		ims.entries = map[string][]byte{key: value}
	} else {
		ims.entries[key] = value
	}
	ims.l.Unlock()
	// fmt.Printf("ims set %s\n", key)
	return nil
}

func (ims *inMemoryStore) Load(ctx context.Context, key string) ([]byte, error) {
	// fmt.Printf("ims %p getting %s...\n", ims, key)
	ims.l.Lock()
	value, ok := ims.entries[key]
	ims.l.Unlock()
	if !ok {
		// fmt.Printf("ims did not ge t%s\n", key)
		return nil, fmt.Errorf("InMemoryStore entry not found for %s", key)
	}
	// fmt.Printf("ims got %s\n", key)
	return value, nil
}
