package mast

import (
	"context"
	"fmt"
)

func ExampleMast_DiffIter() {
	ctx := context.Background()
	v1 := NewInMemory()
	v1.Insert(ctx, 0, "foo")
	v1.Insert(ctx, 100, "asdf")
	v2, err := v1.Clone(ctx)
	if err != nil {
		panic(err)
	}
	v2.Insert(ctx, 0, "bar")
	v2.Delete(ctx, 100, "asdf")
	v2.Insert(ctx, 200, "qwerty")
	v2.DiffIter(ctx, &v1, func(added, removed bool, key, addedValue, removedValue interface{}) (bool, error) {
		if added && removed {
			fmt.Printf("changed '%v'   from '%v' to '%v'\n", key, removedValue, addedValue)
		} else if removed {
			fmt.Printf("removed '%v' value '%v'\n", key, removedValue)
		} else if added {
			fmt.Printf("added   '%v' value '%v'\n", key, addedValue)
		}
		return true, nil
	})
	// Output:
	// changed '0'   from 'foo' to 'bar'
	// removed '100' value 'asdf'
	// added   '200' value 'qwerty'
}

func ExampleMast_Size() {
	ctx := context.Background()
	m := NewInMemory()
	m.Insert(ctx, 0, "zero")
	m.Insert(ctx, 1, "one")
	fmt.Println(m.Size())
	// Output:
	// 2
}
