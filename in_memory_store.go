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
	ims.l.Lock()
	if ims.entries == nil {
		ims.entries = map[string][]byte{key: value}
	} else {
		ims.entries[key] = value
	}
	ims.l.Unlock()
	return nil
}

func (ims *inMemoryStore) Load(ctx context.Context, key string) ([]byte, error) {
	ims.l.Lock()
	value, ok := ims.entries[key]
	ims.l.Unlock()
	if !ok {
		return nil, fmt.Errorf("inMemoryStore entry not found for %s", key)
	}
	return value, nil
}

func (ims *inMemoryStore) NodeURLPrefix() string {
	return fmt.Sprintf("%p", ims)
}
