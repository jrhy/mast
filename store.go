package mast

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/minio/blake2b-simd"
)

type stringNodeT = struct {
	Key   []json.RawMessage
	Value []json.RawMessage
	Link  []string `json:",omitempty"`
}

var (
	defaultUnmarshal = json.Unmarshal
	defaultMarshal   = json.Marshal
)

func (m *Mast) load(link interface{}) (*mastNode, error) {
	switch l := link.(type) {
	case string:
		return m.loadPersisted(l)
	case *mastNode:
		return l, nil
	default:
		return nil, fmt.Errorf("unknown link type %T", l)
	}
}

func (m *Mast) loadPersisted(l string) (*mastNode, error) {
	if m.nodeCache != nil {
		if node, ok := m.nodeCache.Get(l); ok {
			return node.(*mastNode), nil
		}
	}
	nodeBytes, err := m.persist.Load(l)
	if err != nil {
		return nil, fmt.Errorf("persist load %s: %w", l, err)
	}
	var node mastNode
	if !m.unmarshalerUsesRegisteredTypes {
		var stringNode stringNodeT
		err = m.unmarshal(nodeBytes, &stringNode)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling %s: %w", l, err)
		}
		// if m.debug {
		// fmt.Printf("loaded stringNode: %v\n", stringNode)
		// }
		if len(stringNode.Key) != len(stringNode.Value) {
			return nil, fmt.Errorf("cannot unmarshal %s: mismatched keys and values", l)
		}
		node = mastNode{
			make([]interface{}, len(stringNode.Key)),
			make([]interface{}, len(stringNode.Value)),
			make([]interface{}, len(stringNode.Key)+1),
			false, true, nil, &l,
		}
		for i := 0; i < len(stringNode.Key); i++ {
			aType := reflect.TypeOf(m.zeroKey)
			aCopy := reflect.New(aType)
			err := m.unmarshal(stringNode.Key[i], aCopy.Interface())
			if err != nil {
				return nil, fmt.Errorf("cannot unmarshal key[%d] in %s: %w", i, l, err)
			}
			newKey := aCopy.Elem().Interface()

			var newValue interface{}
			if m.zeroValue != nil {
				aType = reflect.TypeOf(m.zeroValue)
				aCopy = reflect.New(aType)
				err = m.unmarshal(stringNode.Value[i], aCopy.Interface())
				if err != nil {
					return nil, fmt.Errorf("cannot unmarshal value[%d] in %s: %w", i, l, err)
				}
				newValue = aCopy.Elem().Interface()
			} else {
				newValue = nil
			}

			node.Key[i] = newKey
			node.Value[i] = newValue
		}
		if stringNode.Link != nil {
			for i, l := range stringNode.Link {
				if l == "" {
					node.Link[i] = nil
				} else {
					node.Link[i] = l
				}
			}
		}
		node.expected = node.xcopy()
	} else {
		err = m.unmarshal(nodeBytes, &node)
		if err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		if node.Link == nil || len(node.Link) == 0 {
			node.Link = make([]interface{}, len(node.Key)+1, m.branchFactor)
		} else if len(node.Link) != len(node.Key)+1 {
			return nil, fmt.Errorf("deserialized wrong number of links")
		}
		node.shared = true
		node.expected = node.xcopy()
		node.source = &l
	}
	if m.debug {
		fmt.Printf("loaded node %s->%v\n", l, node)
	}
	validateNode(&node, m)
	if m.nodeCache != nil {
		m.nodeCache.Add(l, &node)
	}
	return &node, nil
}

func (m *Mast) store(node *mastNode) (interface{}, error) {
	// validateNode(node, m)
	if len(node.Link) == 1 && node.Link[0] == nil {
		return nil, fmt.Errorf("bug! shouldn't be storing empty nodes")
	}
	return node, nil
}

func (node *mastNode) store(persist Persist, cache NodeCache, marshal func(interface{}) ([]byte, error)) (string, error) {
	if !node.dirty {
		if node.expected != nil {
			if !reflect.DeepEqual(node.expected.Key, node.Key) {
				fmt.Printf("expected node %v\n", node.expected)
				fmt.Printf("found    node %v\n", node)
				return "", fmt.Errorf("dangit! node mismatches expected")
			}
			if !reflect.DeepEqual(node.expected.Value, node.Value) {
				fmt.Printf("expected node %v\n", node.expected)
				fmt.Printf("found    node %v\n", node)
				return "", fmt.Errorf("dangit! node mismatches expected")
			}
		}
		if node.source != nil {
			return *node.source, nil
		}
	} else {
		if node.expected != nil {
			if !reflect.DeepEqual(node.expected.Key, node.Key) &&
				reflect.DeepEqual(node.expected.Value, node.Value) &&
				reflect.DeepEqual(node.expected.Link, node.Link) {
				return "", errors.New("dangit! node is not really dirty")
			}
		}
	}

	linkCount := 0
	for i, il := range node.Link {
		if il == nil {
			continue
		}
		linkCount++
		switch l := il.(type) {
		case string:
			break
		case *mastNode:
			newLink, err := l.store(persist, cache, marshal)
			if err != nil {
				return "", fmt.Errorf("flush: %w", err)
			}
			node.Link[i] = newLink
		default:
			return "", fmt.Errorf("don't know how to flush link of type %T", l)
		}
	}
	trimmed := *node
	if linkCount == 0 {
		trimmed.Link = nil
	}
	encoded, err := marshal(trimmed)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	hashBytes := blake2b.Sum256(encoded)
	hash := base64.RawURLEncoding.EncodeToString(hashBytes[:])
	if cache != nil {
		if cache.Contains(hash) {
			return hash, nil
		}
	}
	err = persist.Store(hash, encoded)
	if err != nil {
		return "", fmt.Errorf("persist store: %w", err)
	}
	// fmt.Printf("%s->%s\n", hash, encoded)
	if node.dirty && node.source != nil && *node.source != hash {
		fmt.Printf("expected node %s %v\n", *node.source, node.expected)
		fmt.Printf("found    node %s %v\n", hash, node)
		panic(fmt.Errorf("whoa, somebody modified %v==>%v after loading (keys were %v, became %v)", *node.source, hash, node.expected.Key, node.Key))
	}
	node.dirty = false
	node.expected = node.xcopy()
	node.source = &hash
	node.shared = true
	if cache != nil {
		cache.Add(hash, node)
	}
	return hash, nil
}
