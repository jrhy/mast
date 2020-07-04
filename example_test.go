package mast

import (
	"fmt"
)

func ExampleMast_DiffIter() {
	v1 := NewInMemory()
	v1.Insert(0, "foo")
	v1.Insert(100, "asdf")
	v2 := v1
	v2.Insert(0, "bar")
	v2.Delete(100, "asdf")
	v2.Insert(200, "qwerty")
	v2.DiffIter(&v1, func(added, removed bool, key, addedValue, removedValue interface{}) (bool, error) {
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
	m := NewInMemory()
	m.Insert(0, "zero")
	m.Insert(1, "one")
	fmt.Println(m.Size())
	// Output:
	// 2
}
