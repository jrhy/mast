package mast

import (
	"fmt"
	"reflect"
)

// CreateRemoteOptions sets initial parameters for the tree, that would be painful to change after the tree has data.
type CreateRemoteOptions struct {
	// BranchFactor, or number of entries per node.  0 means use DefaultBranchFactor.
	BranchFactor uint
}

// entry represents a key and value in the tree.
type entry struct {
	Key   interface{}
	Value interface{}
}

// Persist is the interface for loading and storing (serialized) tree nodes. The given string identity corresponds to the content which is immutable (never modified).
type Persist interface {
	// Store makes the given bytes accessible by the given name. The given string identity corresponds to the content which is immutable (never modified).
	Store(string, []byte) error
	// Load retrieves the previously-stored bytes by the given name.
	Load(string) ([]byte, error)
}

// RemoteConfig controls how nodes are persisted and loaded.
type RemoteConfig struct {
	// KeysLike is an instance of the type keys will be deserialized as.
	KeysLike interface{}

	// KeysLike is an instance of the type values will be deserialized as.
	ValuesLike interface{}

	// StoreImmutablePartsWith is used to store and load serialized nodes.
	StoreImmutablePartsWith Persist
	Unmarshal               func([]byte, interface{}) error
}

// Root identifies a version of a tree whose nodes are accessible in the persistent store.
type Root struct {
	Link         *string
	Size         uint64
	Height       uint8
	BranchFactor uint
}

// Delete deletes the entry with given key and value from the tree.
func (m *Mast) Delete(key interface{}, value interface{}) error {
	if m.debug {
		fmt.Printf("deleting %v...\n", key)
	}
	if m.root == nil {
		return fmt.Errorf("key %v not present in tree", key)
	}
	node, err := m.load(m.root)
	if err != nil {
		return fmt.Errorf("load root: %w", err)
	}
	keyLayer, err := m.keyLayer(key, m.branchFactor)
	if err != nil {
		return fmt.Errorf("layer: %w", err)
	}
	options := findOptions{
		targetLayer:        uint8min(keyLayer, m.height),
		currentHeight:      m.height,
		createMissingNodes: false,
		path:               []pathEntry{},
	}
	node, i, err := node.findNode(m, key, &options)
	if err != nil {
		return fmt.Errorf("findNode: %w", err)
	}
	// validateNode(node, mast)
	if options.targetLayer != options.currentHeight ||
		i == len(node.Key) {
		return fmt.Errorf("key %v not present in tree", key)
	}
	cmp, err := m.keyCompare(node.Key[i], key)
	if err != nil {
		return fmt.Errorf("keyCompare: %w", err)
	}
	if cmp != 0 {
		return fmt.Errorf("key %v not present in tree", key)
	}
	if node.Value[i] != value {
		return fmt.Errorf("value not present for given key (found=%v, wanted=%v)", node.Value[i], value)
	}
	oldNode := node
	node = emptyNodePointer()
	node.Key = make([]interface{}, len(oldNode.Key)-1)
	copy(node.Key[:i], oldNode.Key[:i])
	copy(node.Key[i:], oldNode.Key[i+1:])
	node.Value = make([]interface{}, len(oldNode.Value)-1)
	copy(node.Value[:i], oldNode.Value[:i])
	copy(node.Value[i:], oldNode.Value[i+1:])
	node.Link = make([]link, len(oldNode.Link)-1)
	copy(node.Link[:i], oldNode.Link[:i])
	copy(node.Link[i+1:], oldNode.Link[i+2:])
	mergedLink, err := m.mergeNodes(oldNode.Link[i], oldNode.Link[i+1])
	if err != nil {
		return fmt.Errorf("merge: %w", err)
	}
	node.Link[i] = mergedLink
	validateNode(node, m)
	options.path[len(options.path)-1].node = node
	err = m.savePathForRoot(options.path)
	if err != nil {
		return fmt.Errorf("savePathForRoot: %w", err)
	}
	m.size--
	for m.size < m.shrinkBelowSize && m.height > 0 {
		err = m.shrink()
		if err != nil {
			return fmt.Errorf("shrink: %w", err)
		}
	}
	return nil
}

// DiffIter invokes the given callback (exactly once) for every
// entry that is different between this and the given tree.
func (m *Mast) DiffIter(
	oldMast *Mast,
	f func(added, removed bool, key, addedValue, removedValue interface{}) (bool, error),
) error {
	return m.diff(oldMast, f, nil)
}

// DiffLinks invokes the given callback (maybe more than once)
// for every node that is different between this and the given tree.
func (m *Mast) DiffLinks(
	oldMast *Mast,
	f func(removed bool, link link) (bool, error),
) error {
	return m.diff(oldMast, nil, f)
}

// flush serializes changes (new nodes) into the persistent store.
func (m *Mast) flush() (string, error) {
	if m.persist == nil {
		return "", fmt.Errorf("set mast.Persist to a persistence mechanism")
	}
	node, err := m.load(m.root)
	if err != nil {
		return "", fmt.Errorf("loading root: %w", err)
	}
	str, err := node.flush(m.persist, m.marshal)
	if err != nil {
		return "", err
	}
	m.root = contentHash(str)
	return string(str), nil
}

// Get gets the value of the entry with the given key and stores it at the given value pointer. Returns false if the tree doesn't contain the given key.
func (m *Mast) Get(k interface{}, value interface{}) (bool, error) {
	if m.root == nil {
		return false, nil
	}
	node, err := m.load(m.root)
	if err != nil {
		return false, err
	}
	keyLayer, err := m.keyLayer(k, m.branchFactor)
	if err != nil {
		return false, fmt.Errorf("layer: %w", err)
	}
	options := findOptions{
		targetLayer:   uint8min(keyLayer, m.height),
		currentHeight: m.height,
	}
	node, i, err := node.findNode(m, k, &options)
	if err != nil {
		return false, err
	}
	if i >= len(node.Key) ||
		options.targetLayer != options.currentHeight {
		return false, nil
	}
	cmp, err := m.keyCompare(node.Key[i], k)
	if err != nil {
		return false, fmt.Errorf("keyCompare: %w", err)
	}
	if cmp != 0 {
		return false, nil
	}
	if value != nil {
		if node.Value[i] == nil {
			if !reflect.ValueOf(value).IsZero() {
				return false, fmt.Errorf("cannot set return pointer for nil node value")
			}
			return true, nil
		}
		reflect.ValueOf(value).Elem().Set(reflect.ValueOf(node.Value[i]))
	}
	return true, nil
}

// Insert adds or replaces the value for the given key.
func (m *Mast) Insert(key interface{}, value interface{}) error {
	if m.debug {
		fmt.Printf("inserting %v...\n", key)
	}
	keyLayer, err := m.keyLayer(key, m.branchFactor)
	if err != nil {
		return fmt.Errorf("layer: %w", err)
	}
	options := findOptions{
		targetLayer:        uint8min(keyLayer, m.height),
		currentHeight:      m.height,
		createMissingNodes: true,
		path:               []pathEntry{},
	}
	node, err := m.load(m.root)
	if err != nil {
		return err
	}
	node, i, err := node.findNode(m, key, &options)
	if err != nil {
		return err
	}
	if options.targetLayer != options.currentHeight {
		panic("dunno why we didn't land in the right layer")
	}
	if i < len(node.Key) {
		cmp, err := m.keyCompare(node.Key[i], key)
		if err != nil {
			return fmt.Errorf("keyCompare: %w", err)
		}
		if cmp == 0 {
			if node.Value[i] == value {
				return nil
			}
			node = node.copy()
			node.Value[i] = value
			options.path[len(options.path)-1].node = node
			return m.savePathForRoot(options.path)
		}
	}
	oldKeys := node.Key
	node = node.copy()
	node.Key = make([]interface{}, len(oldKeys)+1)
	copy(node.Key, oldKeys[:i])
	node.Key[i] = key
	oldValues := node.Value
	node.Value = make([]interface{}, len(oldValues)+1)
	copy(node.Value, oldValues[:i])
	node.Value[i] = value
	oldLinks := node.Link
	node.Link = make([]link, len(oldLinks)+1)
	copy(node.Link, oldLinks[:i])
	if i < len(node.Key) {
		copy(node.Key[i+1:], oldKeys[i:])
		copy(node.Value[i+1:], oldValues[i:])
		copy(node.Link[i+2:], oldLinks[i+1:])
	}
	var leftLink link
	var rightLink link
	if oldLinks[i] != nil {
		child, err := m.load(oldLinks[i])
		if err != nil {
			return err
		}
		if m.debug {
			fmt.Printf("  doing a split, of node with keys %v\n", child.Key)
		}
		leftLink, rightLink, err = split(child, key, m)
		if err != nil {
			return fmt.Errorf("split: %w", err)
		}
	} else {
		if m.debug {
			fmt.Printf("  child did not need a split\n")
		}
		leftLink = nil
		rightLink = node.Link[i]
	}

	node.Link[i] = leftLink
	node.Link[i+1] = rightLink
	options.path[len(options.path)-1].node = node
	err = m.savePathForRoot(options.path)
	if err != nil {
		return fmt.Errorf("save new root: %w", err)
	}
	for m.size >= m.growAfterSize {
		canGrow, err := options.path[0].node.canGrow(m.height, m.keyLayer, m.branchFactor)
		if err != nil {
			return fmt.Errorf("canGrow: %w", err)
		}
		if !canGrow {
			break
		}
		if m.debug {
			fmt.Printf("before growing:\n")
			m.dump()
		}
		err = m.grow()
		if err != nil {
			return fmt.Errorf("grow: %w", err)
		}
	}
	m.size++
	return nil
}

// Iter iterates over the entries of a tree, invoking the given callback for every entry's key and value.
func (m *Mast) Iter(f func(interface{}, interface{}) error) error {
	node, err := m.load(m.root)
	if err != nil {
		return err
	}
	return node.iter(f, m)
}

// keys returns the keys of the tree's entries as an array.
func (m *Mast) keys() ([]interface{}, error) {
	array := make([]interface{}, m.size)
	i := 0
	err := m.Iter(func(key interface{}, _ interface{}) error {
		array[i] = key
		i++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return array, nil
}

// LoadMast loads a tree from a remote store. The root is loaded
// and verified; other nodes will be loaded on demand.
func (r *Root) LoadMast(config RemoteConfig) (*Mast, error) {
	var link link
	if r.Link != nil {
		link = contentHash(*r.Link)
	} else {
		link = emptyNodePointer()
	}
	shrinkSize := uint64(1)
	for i := 0; i < int(r.Height); i++ {
		shrinkSize *= uint64(r.BranchFactor)
	}
	m := Mast{
		root:            link,
		zeroKey:         config.KeysLike,
		zeroValue:       config.ValuesLike,
		unmarshal:       config.Unmarshal,
		marshal:         defaultMarshal,
		keyCompare:      defaultComparer,
		keyLayer:        defaultLayer,
		branchFactor:    r.BranchFactor,
		height:          r.Height,
		persist:         config.StoreImmutablePartsWith,
		shrinkBelowSize: shrinkSize,
		growAfterSize:   shrinkSize * uint64(r.BranchFactor),
	}
	if config.Unmarshal == nil {
		m.unmarshal = defaultUnmarshal
	}
	err := m.checkRoot()
	if err != nil {
		return nil, fmt.Errorf("checkRoot: %w", err)
	}
	return &m, nil
}

// MakeRoot makes a new persistent root, after ensuring all the changed nodes
// have been written to the persistent store.
func (m *Mast) MakeRoot() (*Root, error) {
	link, err := m.flush()
	if err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}
	return &Root{&link, m.size, m.height, m.branchFactor}, nil
}

// NewInMemory returns a new tree for use as an in-memory data structure
// (i.e. that isn't intended to be remotely persisted).
func NewInMemory() Mast {
	return Mast{
		root:            emptyNodePointer(),
		branchFactor:    DefaultBranchFactor,
		growAfterSize:   DefaultBranchFactor,
		shrinkBelowSize: uint64(1),
		keyCompare:      defaultComparer,
		keyLayer:        defaultLayer,
		unmarshal:       defaultUnmarshal,
		marshal:         defaultMarshal,
	}
}

// NewRoot creates an empty tree whose nodes will be persisted remotely according to remoteOptions.
func NewRoot( /*config RemoteConfig,*/ remoteOptions *CreateRemoteOptions) *Root {
	var branchFactor = uint(DefaultBranchFactor)
	if remoteOptions != nil && remoteOptions.BranchFactor > 0 {
		branchFactor = remoteOptions.BranchFactor
	}
	return &Root{nil, 0, 0, branchFactor}
}

// Size returns the number of entries in the tree.
func (m Mast) Size() uint64 {
	return m.size
}

func (m Mast) string() string {
	node, err := m.load(m.root)
	if err != nil {
		panic(err)
	}
	str, err := node.string("   ", &m)
	if err != nil {
		panic(err)
	}
	return "{\n" + str + "}"
}

// toSlice returns an array of the tree's entries.
func (m Mast) toSlice() ([]entry, error) {
	array := make([]entry, m.size)
	i := 0
	err := m.Iter(func(key interface{}, value interface{}) error {
		array[i] = entry{key, value}
		i++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return array, nil
}

// values returns the values of the tree's entries as an array.
func (m Mast) values() (interface{}, error) {
	array := make([]interface{}, m.size)
	i := 0
	err := m.Iter(func(_ interface{}, value interface{}) error {
		array[i] = value
		i++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return array, nil
}
