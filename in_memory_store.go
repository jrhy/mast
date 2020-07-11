package mast

import (
	"fmt"
)

type inMemoryStore struct {
	entries map[string][]byte
}

// NewInMemoryStore provides a Persist that stores serialized nodes in a map, usually for testing.
func NewInMemoryStore() Persist {
	return inMemoryStore{make(map[string][]byte)}
}

func (ims inMemoryStore) Store(key string, value []byte) error {
	ims.entries[key] = value
	return nil
}

func (ims inMemoryStore) Load(key string) ([]byte, error) {
	value, ok := ims.entries[key]
	if !ok {
		return nil, fmt.Errorf("InMemoryStore entry not found for %s", key)
	}
	return value, nil
}
