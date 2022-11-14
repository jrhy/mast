package mast

import (
	"bytes"
	"fmt"
	"hash/crc64"
	"reflect"
)

// A Key has a sort order and deterministic maximum distance from leaves.
type Key interface {
	// Layer can deterministically compute its ideal layer (distance from leaves) in a tree with the given branch factor.
	Layer(branchFactor uint) uint8
	// Order returns -1 if the argument is less than than this one, 1 if greater, and 0 if equal.
	Order(Key) int
}

var crcTable *crc64.Table = crc64.MakeTable(crc64.ECMA)

func DefaultKeyCompare(marshaler func(interface{}) ([]byte, error)) func(i, i2 interface{}) (int, error) {
	return func(i, i2 interface{}) (int, error) {
		switch v := i.(type) {
		case Key:
			if v2, ok := i2.(Key); ok {
				return v.Order(v2), nil
			}
		case string:
			if v2, ok := i2.(string); ok {
				if v < v2 {
					return -1, nil
				} else if v > v2 {
					return 1, nil
				}
				return 0, nil
			}
		case int:
			if v2, ok := i2.(int); ok {
				if v < v2 {
					return -1, nil
				} else if v > v2 {
					return 1, nil
				}
				return 0, nil
			}
		case uint:
			if v2, ok := i2.(uint); ok {
				if v < v2 {
					return -1, nil
				} else if v > v2 {
					return 1, nil
				}
				return 0, nil
			}
		case uint64:
			if v2, ok := i2.(uint64); ok {
				if v < v2 {
					return -1, nil
				} else if v > v2 {
					return 1, nil
				}
				return 0, nil
			}
		case int64:
			if v2, ok := i2.(int64); ok {
				if v < v2 {
					return -1, nil
				} else if v > v2 {
					return 1, nil
				}
				return 0, nil
			}
		case []byte:
			if v2, ok := i2.([]byte); ok {
				return bytes.Compare(v, v2), nil
			}
		default:
			if reflect.TypeOf(v) != reflect.TypeOf(i2) {
				return -1, fmt.Errorf("don't know how to compare %T with %T; set Mast.keyOrder or implement Key interface", i, i2)
			}
			b, err := marshaler(i)
			if err != nil {
				return -1, fmt.Errorf("marshal left: %w", err)
			}
			b2, err := marshaler(i2)
			if err != nil {
				return -1, fmt.Errorf("marshal right: %w", err)
			}
			return bytes.Compare(b, b2), nil
		}
		return -1, fmt.Errorf("don't know how to compare %T with %T; set Mast.keyOrder or implement Key interface", i, i2)
	}
}

func defaultLayer(marshaler func(interface{}) ([]byte, error)) func(i interface{}, branchFactor uint) (uint8, error) {
	return func(i interface{}, branchFactor uint) (uint8, error) {
		switch v := i.(type) {
		case Key:
			return v.Layer(branchFactor), nil
		case []byte:
			return blobLayer(v, branchFactor), nil
		case string:
			return stringLayer(v, branchFactor), nil
		case int:
			return intLayer(int64(v), branchFactor), nil
		case int8:
			return intLayer(int64(v), branchFactor), nil
		case int16:
			return intLayer(int64(v), branchFactor), nil
		case int32:
			return intLayer(int64(v), branchFactor), nil
		case int64:
			return intLayer(v, branchFactor), nil
		case uint:
			return uintLayer(uint64(v), branchFactor), nil
		case uint8:
			return uintLayer(uint64(v), branchFactor), nil
		case uint16:
			return uintLayer(uint64(v), branchFactor), nil
		case uint32:
			return uintLayer(uint64(v), branchFactor), nil
		case uint64:
			return uintLayer(v, branchFactor), nil
		}
		b, err := marshaler(i)
		if err != nil {
			return 0, err
		}
		return blobLayer(b, branchFactor), nil
	}
}

func intLayer(v int64, branchFactor uint) uint8 {
	layer := uint8(0)
	for ; v != 0 && v%int64(branchFactor) == 0; layer++ {
		v /= int64(branchFactor)
	}
	return layer
}

func uintLayer(v uint64, branchFactor uint) uint8 {
	layer := uint8(0)
	for ; v != 0 && v%uint64(branchFactor) == 0; layer++ {
		v /= uint64(branchFactor)
	}
	return layer
}

func stringLayer(s string, branchFactor uint) uint8 {
	return blobLayer([]byte(s), branchFactor)
}
func blobLayer(b []byte, branchFactor uint) uint8 {
	return uintLayer(crc64.Checksum(b, crcTable), branchFactor)
}
