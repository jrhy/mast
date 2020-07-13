package mast

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/arbitrary"
	"github.com/leanovate/gopter/gen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultGopterParameters = gopter.DefaultTestParameters()

func newTestTree(zeroKey interface{}, zeroValue interface{}) Mast {
	return Mast{
		root:            emptyNodePointer(),
		zeroKey:         zeroKey,
		zeroValue:       zeroValue,
		branchFactor:    DefaultBranchFactor,
		growAfterSize:   DefaultBranchFactor,
		shrinkBelowSize: uint64(1),
		persist:         NewInMemoryStore(),
		keyOrder:        defaultOrder,
		keyLayer:        defaultLayer,
		unmarshal:       defaultUnmarshal,
		marshal:         defaultMarshal,
	}
}

func TestNew(t *testing.T) {
	m := NewInMemory()
	require.Equal(t, uint64(0), m.Size())
	root, err := m.load(m.root)
	require.NoError(t, err, "failed to load root")
	require.Equal(t, 1, len(root.Link))
}

func TestSplit(t *testing.T) {
	m := NewInMemory()
	node := mastNode{
		Key:   []interface{}{10, 20, 30},
		Value: []interface{}{"", "", ""},
		Link:  []interface{}{nil, nil, nil, nil},
	}
	newLeftLink, newRightLink, err := split(&node, 15, &m)
	require.NoError(t, err)
	newLeft, err := m.load(newLeftLink)
	require.NoError(t, err)
	newRight, err := m.load(newRightLink)
	require.NoError(t, err)
	require.Equal(t, []interface{}{10}, newLeft.Key)
	require.Equal(t, []interface{}{20, 30}, newRight.Key)
}

func TestInsert(t *testing.T) {
	m := NewInMemory()
	err := m.Insert(50, 50)
	require.NoError(t, err)
	node, err := m.load(m.root)
	require.NoError(t, err)
	require.Equal(t, []interface{}{50}, node.Key)
	require.Equal(t, []interface{}{50}, node.Value)
	require.Equal(t, []interface{}{nil, nil}, node.Link)
	require.Equal(t, uint64(1), m.size)
	require.Equal(t, uint8(0), m.height)
	err = m.Insert(40, 40)
	require.NoError(t, err)
	node, err = m.load(m.root)
	require.NoError(t, err)
	require.Equal(t, []interface{}{40, 50}, node.Key)
	require.Equal(t, []interface{}{40, 50}, node.Value)
	require.Equal(t, []interface{}{nil, nil, nil}, node.Link)
	require.Equal(t, uint64(2), m.size)
	require.Equal(t, uint8(0), m.height)

	err = m.Insert(60, 60)
	require.NoError(t, err)
	node, err = m.load(m.root)
	require.NoError(t, err)
	require.Equal(t, []interface{}{40, 50, 60}, node.Key)
	require.Equal(t, []interface{}{40, 50, 60}, node.Value)
	require.Equal(t, []interface{}{nil, nil, nil, nil}, node.Link)
	require.Equal(t, uint64(3), m.size)
	require.Equal(t, uint8(0), m.height)

	err = m.Insert(45, 45)
	require.NoError(t, err)
	node, err = m.load(m.root)
	require.NoError(t, err)
	require.Equal(t, []interface{}{40, 45, 50, 60}, node.Key)
	require.Equal(t, []interface{}{40, 45, 50, 60}, node.Value)
	require.Equal(t, []interface{}{nil, nil, nil, nil, nil}, node.Link)
	require.Equal(t, uint64(4), m.size)
	require.Equal(t, uint8(0), m.height)
}

func TestInsertGrow(t *testing.T) {
	m := NewInMemory()
	for i := 1; i < 17; i++ {
		var err error
		if i == 16 {
			err = m.Insert(i*10, 0)
		} else {
			err = m.Insert(i*10+1, 0)
		}
		require.NoError(t, err, "failed to insert %d", i)
	}
	require.Equal(t, uint64(16), m.size)
	require.Equal(t, uint8(0), m.height)
	i := 17
	err := m.Insert(i*10+1, 0)
	require.NoError(t, err, "failed to insert %d", i)
	require.Equal(t, uint64(17), m.size)
	require.Equal(t, uint8(1), m.height)
	node, err := m.load(m.root)
	require.NoError(t, err)
	require.Equal(t, 2, len(node.Link))
	for i := 1; i < 18; i++ {
		var n int
		n = i*10 + 1
		if i == 16 {
			n = 160
		}
		contains, err := m.contains(n)
		require.Nil(t, err)
		require.True(t, contains)
	}
}

func TestInsertSplit(t *testing.T) {
	m := NewInMemory()
	for i := 1; i < 17; i++ {
		var err error
		if i == 16 {
			err = m.Insert(i*10, 0)
		} else {
			err = m.Insert(i*10+1, 0)
		}
		require.NoError(t, err, "failed to insert %d", i)
	}
	require.Equal(t, uint64(16), m.size)
	require.Equal(t, uint8(0), m.height)
	i := 171
	err := m.Insert(i, 0)
	require.NoError(t, err, "failed to insert %d", i)
	i = 80
	err = m.Insert(i, 0)
	require.NoError(t, err)
}

func TestToSlice(t *testing.T) {
	m := NewInMemory()
	m.Insert(3, 0)
	m.Insert(1, 0)
	m.Insert(2, 0)
	expected := []entry{
		{1, 0},
		{2, 0},
		{3, 0},
	}
	actual, err := m.toSlice()
	require.Nil(t, err)
	require.Equal(t, expected, actual)
}

func TestKeys(t *testing.T) {
	m := NewInMemory()
	m.Insert(3, 0)
	m.Insert(1, 0)
	m.Insert(2, 0)
	expected := []interface{}{1, 2, 3}
	actual, err := m.keys()
	require.Nil(t, err)
	require.Equal(t, expected, actual)
}

func checkRecall(t *testing.T, to []TestOperation) bool {
	m := newTestTree(0, uint(0))
	expected := make(map[uint]uint)
	for i, op := range to {
		err := m.apply(to[i : i+1])
		require.NoError(t, err)
		actual := make(map[uint]uint)
		err = m.Iter(func(key interface{}, value interface{}) error {
			intKey := key.(uint)
			uintValue := value.(uint)
			actual[uint(intKey)] = uintValue
			return nil
		})
		expected[op.Key] = op.Value
		assert.Equal(t, len(expected), int(m.size))
		equal := reflect.DeepEqual(expected, actual)
		assert.True(t, equal, "failed at op=%d %v", i, op)
		if !equal {
			fmt.Printf("test operations: %v\n", to)
		}
		assert.Equal(t, expected, actual)
		if !equal {
			fmt.Printf("after:\n")
			m.dump()
			return false
		}
	}
	return true
}

func checkRecallPow4(t *testing.T, to []TestOperation) bool {
	m := newTestTree(uint(0), uint(0))
	m.branchFactor = 4
	m.growAfterSize = 4
	expected := make(map[uint]uint)
	var i int
	var op TestOperation
	for i, op = range to {
		err := m.Insert(op.Key, op.Value)
		require.NoError(t, err)
		expected[op.Key] = op.Value
	}

	actual := make(map[uint]uint)
	err := m.Iter(func(key interface{}, value interface{}) error {
		pow4Key := key.(uint)
		uintValue := value.(uint)
		actual[uint(pow4Key)] = uintValue
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, len(expected), int(m.size))
	equal := reflect.DeepEqual(expected, actual)
	assert.True(t, equal, "failed at op=%d %v", i, op)
	if !equal {
		fmt.Printf(`func TestRecallExample(t *testing.T) {
				c := []TestOperation{
					`)
		for _, op := range to {
			fmt.Printf("{%d,%d}, ", op.Key, op.Value)
		}
		fmt.Printf(`
				}
				checkRecallPow4(t, c)
			}
			`)
	}
	assert.Equal(t, expected, actual)
	if !equal {
		fmt.Printf("after:\n")
		m.dump()
		return false
	}
	return true
}

func TestRecall(t *testing.T) {
	properties := gopter.NewProperties(defaultGopterParameters)
	arbitraries := arbitrary.DefaultArbitraries()
	arbitraries.RegisterGen(gen.UIntRange(0, 10_000))

	properties.Property("get every put",
		arbitraries.ForAll(
			func(to []TestOperation) bool {
				return checkRecallPow4(t, to)
			}))
	properties.TestingRun(t)
}

func TestCongruence(t *testing.T) {
	properties := gopter.NewProperties(defaultGopterParameters)
	arbitraries := arbitrary.DefaultArbitraries()
	baseTree := newTestTree(uint(0), "")
	baseTree.branchFactor = 4
	baseTree.growAfterSize = 4

	properties.Property("trees look the same no matter what order the insertions are done",
		arbitraries.ForAll(
			func(uintKeys []uint) bool {
				var keys []interface{}
				for _, key := range uintKeys {
					keys = append(keys, key)
				}
				return checkCongruence(t, baseTree, keys)
			}))
	properties.TestingRun(t)
}

func (root *Mast) apply(to []TestOperation) error {
	for _, to := range to {
		err := root.Insert(to.Key, to.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

type TestOperation struct {
	Key   uint
	Value uint
}

type operation int

const (
	Insert operation = iota
	Delete
)

/*
func testOperations(n int) []TestOperation {
	res := make([]TestOperation, n)
	for i := 0; i < n; i++ {
		var operation operation
		if rand.Int()%2 == 0 {
			operation = Insert
		} else {
			operation = Delete
		}
		testOperation := TestOperation{
			operation,
			rand.Int31(),
			rand.Int31(),
		}
		res[i] = testOperation
	}
	return res
}
*/

func TestContentHash(t *testing.T) {
	m := newTestTree(0, "")
	err := m.Insert(1, "one")
	require.NoError(t, err)
	hash1, err := m.flush()
	require.NoError(t, err)
	m = newTestTree(0, "")
	err = m.Insert(2, "two")
	require.NoError(t, err)
	hash2, err := m.flush()
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash2)
	m = newTestTree(0, "")
	err = m.Insert(2, "two")
	require.NoError(t, err)
	hash2b, err := m.flush()
	require.NoError(t, err)
	require.Equal(t, hash2b, hash2)
}

func TestContentHash_DiffersOnUpsert(t *testing.T) {
	m := newTestTree(0, "")
	err := m.Insert(1, "one")
	require.NoError(t, err)
	hash1, err := m.flush()
	require.NoError(t, err)
	m = newTestTree(0, "")
	err = m.Insert(2, "two")
	require.NoError(t, err)
	hash2, err := m.flush()
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash2)
	m = newTestTree(0, "")
	err = m.Insert(2, "TWO")
	require.NoError(t, err)
	hash2b, err := m.flush()
	require.NoError(t, err)
	require.NotEqual(t, hash2b, hash2)
}

func TestEmptyLeavesRecall(t *testing.T) {
	const testLen = 300
	recallCase := make([]TestOperation, testLen)
	for i := 0; i < testLen; i++ {
		n := uint(i * 16)
		recallCase[i] = TestOperation{n, n}
	}
	checkRecall(t, recallCase)
}

func TestEmptyLeavesCongruence(t *testing.T) {
	const testLen = 300
	congruenceCase := make([]interface{}, testLen)
	for i := 0; i < testLen; i++ {
		n := uint(i * 16)
		congruenceCase[i] = n
	}
	checkCongruence(t, newTestTree(uint(0), ""), congruenceCase)
}

func TestEmptyTwoBottomLayersRecall(t *testing.T) {
	const testLen = 300
	recallCase := make([]TestOperation, testLen)
	recallCase = append(recallCase, TestOperation{32, 0}, TestOperation{33, 0}, TestOperation{1, 0}, TestOperation{256, 129})
	for i := 0; i < testLen; i++ {
		n := uint(i * 256)
		recallCase[i] = TestOperation{n, n}
	}
	checkRecall(t, recallCase)
}

func TestEmptyTwoBottomLayersCongruence(t *testing.T) {
	const testLen = 300
	congruenceCase := make([]interface{}, testLen)
	for i := 0; i < testLen; i++ {
		n := uint(i * 256)
		congruenceCase[i] = n
	}
	checkCongruence(t, newTestTree(uint(0), ""), congruenceCase)
}

func TestEmptyMiddleLayerRecall(t *testing.T) {
	const testLen = 300
	recallCase := make([]TestOperation, testLen)
	for i := 0; i < testLen; i++ {
		var n uint = uint(i)
		if i%16 == 0 {
			n = uint(i * 16)
		}
		recallCase[i] = TestOperation{n, n}
	}
	checkRecall(t, recallCase)
}

func TestEmptyMiddleLayerCongruence(t *testing.T) {
	const testLen = 300
	congruenceCase := make([]interface{}, testLen)
	for i := 0; i < testLen; i++ {
		var n uint = uint(i)
		if i%16 == 0 {
			n = uint(i * 16)
		}
		congruenceCase[i] = n
	}
	checkCongruence(t, newTestTree(uint(0), ""), congruenceCase)
}

func TestEmptyMiddle2LayersRecall(t *testing.T) {
	const testLen = 300
	recallCase := make([]TestOperation, testLen)
	for i := 0; i < testLen; i++ {
		var n uint = uint(i)
		if i%16 == 0 {
			n = uint(i * 256)
		}
		recallCase[i] = TestOperation{n, n}
	}
	checkRecall(t, recallCase)
}

func TestEmptyMiddle2LayersCongruence(t *testing.T) {
	const testLen = 300
	congruenceCase := make([]interface{}, testLen)
	for i := 0; i < testLen; i++ {
		var n uint = uint(i)
		if i%16 == 0 {
			n = uint(i * 256)
		}
		congruenceCase[i] = n
	}
	checkCongruence(t, newTestTree(uint(0), ""), congruenceCase)
}

func TestInterestingZeroCase(t *testing.T) {
	const testLen = 257
	recallCase := make([]TestOperation, testLen)
	for i := 0; i < testLen; i++ {
		n := uint(i * 256)
		recallCase[i] = TestOperation{n, n}
	}
	recallCase = append(recallCase, TestOperation{32, 0}, TestOperation{33, 0}, TestOperation{0, 0})
	checkRecall(t, recallCase)
}

func TestDiffTrivial(t *testing.T) {
	m := newTestTree(0, 0)
	m.Insert(1, 1)
	m2 := newTestTree(0, 0)
	m2.Insert(1, 1)
	m2.Insert(2, 2)
	n := 0
	m2.DiffIter(&m, func(added bool, removed bool, key interface{}, addedValue interface{}, removedValue interface{}) (bool, error) {
		assert.True(t, added)
		assert.False(t, removed)
		n++
		assert.Equal(t, n, 1)
		assert.Equal(t, 2, key)
		assert.Equal(t, 2, addedValue.(int))
		return true, nil
	})
	n = 0
	m.DiffIter(&m2, func(added bool, removed bool, key interface{}, addedValue interface{}, removedValue interface{}) (bool, error) {
		assert.False(t, added)
		assert.True(t, removed)
		n++
		assert.Equal(t, n, 1)
		assert.Equal(t, 2, key)
		assert.Equal(t, 2, removedValue.(int))
		return true, nil
	})
}

func TestDiffToMidpoint(t *testing.T) {
	properties := gopter.NewProperties(defaultGopterParameters)
	arbitraries := arbitrary.DefaultArbitraries()

	properties.Property("diff midpoint to endpoint",
		arbitraries.ForAll(
			func(midpointOps []TestOperation, endpointOps []TestOperation) bool {
				endpointOps = append(midpointOps, endpointOps...)
				return checkDiff(t, midpointOps, endpointOps)
			}))
	properties.TestingRun(t)
}

func TestDiffSkipsUnchangedTree(t *testing.T) {
	skipCase := make([]TestOperation, 256)
	for i := range skipCase {
		skipCase[i] = TestOperation{uint(i), 0}
	}
	checkDiff(t, skipCase[0:len(skipCase)/2], skipCase[0:])
}

func checkDiff(t *testing.T, oldOps []TestOperation, newOps []TestOperation) bool {
	old := newTestTree(uint(0), uint(0))
	err := old.apply(oldOps)
	require.NoError(t, err)

	new := newTestTree(uint(0), uint(0))
	err = new.apply(newOps)
	require.NoError(t, err)

	expectednew := make(map[interface{}]interface{})
	expectedold := make(map[interface{}]interface{})
	for _, op := range oldOps {
		expectedold[op.Key] = op.Value
	}
	for _, op := range newOps {
		expectednew[op.Key] = op.Value
	}
	expectedDiffs := make(map[interface{}]interface{})
	for key, value := range expectednew {
		if expectedold[key] != value {
			expectedDiffs[key] = value
		}
	}
	actualDiffs := make(map[interface{}]interface{})
	_, err = new.flush()
	require.NoError(t, err)
	_, err = old.flush()
	require.NoError(t, err)
	new.debug = false
	new.DiffIter(&old, func(added bool, removed bool, key interface{}, addedValue interface{}, removedValue interface{}) (bool, error) {
		if added {
			actualDiffs[key] = addedValue
		} else if removed {
			actualDiffs[key] = removedValue
		}
		return true, nil
	})
	if !reflect.DeepEqual(expectedDiffs, actualDiffs) {
		fmt.Printf("checkDiff, oldOps=%v, newOps=%v\n", oldOps, newOps)

		fmt.Printf("midpoint tree:\n")
		old.dump()
		fmt.Printf("new tree:\n")
		new.dump()
		assert.Equal(t, expectedDiffs, actualDiffs)
		return false
	}
	return true
}

func TestTreeAssignmentsWorkForVersioning(t *testing.T) {
	m1 := newTestTree(0, 0)
	m1.Insert(1, 1)
	m1.Insert(2, 2)
	m2 := m1
	m2.Insert(3, 3)
	m2.Insert(4, 4)
	assert.Equal(t, uint64(2), m1.Size())
	assert.Equal(t, uint64(4), m2.Size())
}

type arbitraryLayerInt struct {
	Key           int
	AssignedLayer uint8
}

func (me arbitraryLayerInt) Order(other Key) int {
	if otherArbitraryLayerInt, ok := other.(arbitraryLayerInt); ok {
		return me.Key - otherArbitraryLayerInt.Key
	}
	panic(fmt.Sprintf("can't compare with %T", other))
}

func (me arbitraryLayerInt) Layer(branchFactor uint) uint8 {
	return me.AssignedLayer
}

func TestSplitWithoutSlide(t *testing.T) {
	var keys []interface{}
	for _, key := range []arbitraryLayerInt{
		{20, 1},
		{23, 0},
		{27, 0},
		{30, 1},
		{25, 1},
	} {
		keys = append(keys, key)
	}
	tree := newTestTree(arbitraryLayerInt{0, 0}, "")
	tree.growAfterSize = 2
	tree.shrinkBelowSize = 100
	checkCongruence(t, tree, keys)
}

func TestSplitWithSlide(t *testing.T) {
	// TODO how about checking congruence for all permutations of levels?
	var keys []interface{}
	for _, key := range []arbitraryLayerInt{
		{20, 2},
		{23, 1},
		{24, 0},
		{0, 0},
		{30, 2},
		{27, 1},
		{26, 0},
		{25, 2},
	} {
		keys = append(keys, key)
	}
	tree := newTestTree(arbitraryLayerInt{0, 0}, "")
	tree.growAfterSize = 2
	tree.branchFactor = 2

	checkCongruence(t, tree, keys)
}

func checkCongruence(t *testing.T, baseTree Mast, keys []interface{}) bool {
	m := baseTree
	m2 := baseTree
	for _, key := range keys {
		err := m.Insert(key, "")
		assert.NoError(t, err)
		if err != nil {
			return false
		}
	}
	if m.debug {
		fmt.Printf("m: (height %d, size %d, growAfter %d)\n", m.height, m.size, m.growAfterSize)
		m.dump()
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	for _, key := range keys {
		err := m2.Insert(key, "")
		assert.NoError(t, err)
		if err != nil {
			return false
		}
	}
	if m2.debug {
		fmt.Printf("m2: (height %d, size %d, growAfter %d)\n", m2.height, m2.size, m2.growAfterSize)
		m2.dump()
	}

	for _, key := range keys {
		contains, err := m.contains(key)
		assert.NoError(t, err)
		assert.True(t, contains, "m expected to contain %v", key)
		contains, err = m2.contains(key)
		assert.NoError(t, err)
		assert.True(t, contains, "m2 expected to contain %v", key)
	}

	hash, err := m.flush()
	assert.NoError(t, err)
	if err != nil {
		return false
	}
	hash2, err := m2.flush()
	assert.NoError(t, err)
	if err != nil {
		return false
	}

	assert.Equal(t, hash, hash2)
	if hash != hash2 {
		return false
	}

	// now do the deletions, verifying the expected entries are still available
	ok := true
	seenKeys := map[interface{}]bool{}
	filteredKeys := []interface{}{}
	for _, key := range keys {
		if _, seen := seenKeys[key]; seen {
			continue
		}
		filteredKeys = append(filteredKeys, key)
		seenKeys[key] = true
	}
	keys = filteredKeys

	for i, key := range keys {
		err := m.Delete(key, "")
		assert.NoError(t, err)
		if err != nil {
			return false
		}
		for _, key := range keys[:i+1] {
			contains, err := m.contains(key)
			require.NoError(t, err)
			ok = ok && assert.False(t, contains, "m expected to not contain %v", key)
		}
		for _, key := range keys[i+1:] {
			contains, err := m.contains(key)
			require.NoError(t, err)
			ok = ok && assert.True(t, contains, "m expected to contain %v", key)
		}
	}

	return ok
}

func TestCongruenceExample(t *testing.T) {
	m := newTestTree(uint(0), "")
	m.branchFactor = 4
	m.growAfterSize = 4
	var keys []interface{}
	for _, key := range []uint{0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 9, 12, 11, 16, 13, 14, 25} {
		keys = append(keys, key)
	}
	checkCongruence(t, m, keys)
}

func TestRemoteExample(t *testing.T) {
	inMemoryStore := NewInMemoryStore()
	remoteConfig := RemoteConfig{
		KeysLike:                1234,
		ValuesLike:              "hi",
		StoreImmutablePartsWith: inMemoryStore,
	}
	root := NewRoot(nil)
	m, err := root.LoadMast(remoteConfig)
	require.NoError(t, err)
	err = m.Insert(5, "yay")
	require.NoError(t, err)
	root, err = m.MakeRoot()
	require.NoError(t, err)
	m, err = root.LoadMast(remoteConfig)
	require.NoError(t, err)
	var value string
	contains, err := m.Get(5, &value)
	require.True(t, contains)
	require.Equal(t, "yay", value)
}

func TestStructValues(t *testing.T) {
	type foo struct {
		Asdf string
		Q    bool
	}
	remoteConfig := RemoteConfig{
		KeysLike:                1234,
		ValuesLike:              foo{},
		StoreImmutablePartsWith: NewInMemoryStore(),
	}
	root := NewRoot(nil)
	m, err := root.LoadMast(remoteConfig)
	require.NoError(t, err)
	err = m.Insert(5, foo{"a", true})
	require.NoError(t, err)
	root, err = m.MakeRoot()
	require.NoError(t, err)
	m, err = root.LoadMast(remoteConfig)
	require.NoError(t, err)
	var value foo
	contains, err := m.Get(5, &value)
	require.True(t, contains)
	require.Equal(t, foo{"a", true}, value)
}

func TestStringKeys(t *testing.T) {
	m, err := NewRoot(nil).LoadMast(RemoteConfig{
		KeysLike:                "hi",
		ValuesLike:              5,
		StoreImmutablePartsWith: NewInMemoryStore(),
	})
	require.NoError(t, err)
	require.NoError(t, m.Insert("hey", 123))
	var v int

	// without flushing
	contains, err := m.Get("hey", &v)
	require.NoError(t, err)
	require.True(t, contains)
	contains, err = m.Get("nonexistent", &v)
	require.NoError(t, err)
	require.False(t, contains)

	_, err = m.flush()
	require.NoError(t, err)

	// same after loading
	contains, err = m.Get("hey", &v)
	require.NoError(t, err)
	require.True(t, contains)
	contains, err = m.Get("nonexistent", &v)
	require.NoError(t, err)
	require.False(t, contains)
}

func TestNilValues(t *testing.T) {
	m, err := NewRoot(nil).LoadMast(RemoteConfig{
		KeysLike:                "hi",
		ValuesLike:              nil,
		StoreImmutablePartsWith: NewInMemoryStore(),
	})
	require.NoError(t, err)
	require.NoError(t, m.Insert("hey", "zazz"))

	// without flushing
	contains, err := m.Get("hey", nil)
	require.NoError(t, err)
	require.True(t, contains)
	contains, err = m.Get("nonexistent", nil)
	require.NoError(t, err)
	require.False(t, contains)

	_, err = m.flush()
	require.NoError(t, err)

	// same after loading
	contains, err = m.Get("hey", nil)
	require.NoError(t, err)
	require.True(t, contains)
	contains, err = m.Get("nonexistent", nil)
	require.NoError(t, err)
	require.False(t, contains)
}
