package mast

import (
	"context"
	"fmt"
	"reflect"
	"sync"
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
	Store(context.Context, string, []byte) error
	// Load retrieves the previously-stored bytes by the given name.
	Load(context.Context, string) ([]byte, error)
}

// RemoteConfig controls how nodes are persisted and loaded.
type RemoteConfig struct {
	// KeysLike is an instance of the type keys will be deserialized as.
	KeysLike interface{}

	// KeysLike is an instance of the type values will be deserialized as.
	ValuesLike interface{}

	// StoreImmutablePartsWith is used to store and load serialized nodes.
	StoreImmutablePartsWith Persist

	// Unmarshal function, defaults to JSON
	Unmarshal func([]byte, interface{}) error

	// Marshal function, defaults to JSON
	Marshal func(interface{}) ([]byte, error)

	// UnmarshalerUsesRegisteredTypes indicates that the unmarshaler will know how to deserialize an
	// interface{} for a key/value in an entry.  By default, JSON decoding doesn't do this, so is done
	// in two stages, the first to a JsonRawMessage, the second to the actual key/value type.
	UnmarshalerUsesRegisteredTypes bool

	// NodeCache caches deserialized nodes and may be shared across multiple trees.
	NodeCache NodeCache
}

// Root identifies a version of a tree whose nodes are accessible in the persistent store.
type Root struct {
	Link         *string
	Size         uint64
	Height       uint8
	BranchFactor uint
}

// Delete deletes the entry with given key and value from the tree.
func (m *Mast) Delete(ctx context.Context, key, value interface{}) error {
	if m.debug {
		fmt.Printf("deleting %v...\n", key)
	}
	if m.root == nil {
		return fmt.Errorf("key %v not present in tree", key)
	}
	node, err := m.load(ctx, m.root)
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
	node, i, err := node.findNode(ctx, m, key, &options)
	if err != nil {
		return fmt.Errorf("findNode: %w", err)
	}
	if options.targetLayer != options.currentHeight ||
		i == len(node.Key) {
		return fmt.Errorf("key %v not present in tree", key)
	}
	cmp, err := m.keyOrder(node.Key[i], key)
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
	mergedLink, err := m.mergeNodes(ctx, oldNode.Link[i], oldNode.Link[i+1])
	if err != nil {
		return fmt.Errorf("merge: %w", err)
	}
	node = node.ToMut(ctx, m)
	node.Dirty()
	node.Key = append(node.Key[:i], node.Key[i+1:]...)
	node.Value = append(node.Value[:i], node.Value[i+1:]...)
	node.Link = append(node.Link[:i], node.Link[i+1:]...)
	node.Link[i] = mergedLink
	options.path[len(options.path)-1].node = node
	err = m.savePathForRoot(ctx, options.path)
	if err != nil {
		return fmt.Errorf("savePathForRoot: %w", err)
	}
	m.size--
	for m.size < m.shrinkBelowSize && m.height > 0 {
		err = m.shrink(ctx)
		if err != nil {
			return fmt.Errorf("shrink: %w", err)
		}
	}
	return nil
}

// DiffIter invokes the given callback for every entry that is different from the given tree. The
// iteration will stop if the callback returns keepGoing==false or an error. Callback invocation
// with added==removed==false signifies entries whose values have changed.
func (m *Mast) DiffIter(
	ctx context.Context,
	oldMast *Mast,
	f func(added, removed bool,
		key, addedValue, removedValue interface{},
	) (bool, error),
) error {
	return m.diff(ctx, oldMast, f, nil)
}

// DiffLinks invokes the given callback for every node that is different from the given tree. The iteration will stop if the callback returns keepGoing==false or an error.
func (m *Mast) DiffLinks(
	ctx context.Context,
	oldMast *Mast,
	f func(removed bool, link interface{}) (bool, error),
) error {
	return m.diff(ctx, oldMast, nil, f)
}

// flush serializes changes (new nodes) into the persistent store.
func (m *Mast) flush(ctx context.Context) (string, error) {
	if m.persist == nil {
		return "", fmt.Errorf("no persistence mechanism set; set RemoteConfig.StoreImmutablePartsWith")
	}
	node, err := m.load(ctx, m.root)
	if err != nil {
		return "", fmt.Errorf("load root: %w", err)
	}
	storeQ := make(chan func() error)
	n := 40
	gate := make(chan interface{}, n)
	for i := 0; i < n; i++ {
		gate <- nil
	}
	seLock := sync.Mutex{}
	var firstStoreError error
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			f := <-storeQ
			<-gate
			if f == nil {
				break
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { gate <- nil }()
				seLock.Lock()
				if firstStoreError != nil {
					seLock.Unlock()
					return
				}
				seLock.Unlock()
				err := f()
				if err != nil {
					seLock.Lock()
					if firstStoreError == nil {
						firstStoreError = err
					}
					seLock.Unlock()
				}
			}()
		}
		wg.Done()
	}()

	str, err := node.store(ctx, m.persist, m.nodeCache, m.marshal, storeQ)
	close(storeQ)
	wg.Wait()
	if err != nil {
		return "", err
	}
	if firstStoreError != nil {
		return "", firstStoreError
	}
	m.root = str
	return str, nil
}

// Get gets the value of the entry with the given key and stores it at the given value pointer. Returns false if the tree doesn't contain the given key.
func (m *Mast) Get(ctx context.Context, k, value interface{}) (bool, error) {
	if m.root == nil {
		return false, nil
	}
	node, err := m.load(ctx, m.root)
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
	node, i, err := node.findNode(ctx, m, k, &options)
	if err != nil {
		return false, err
	}
	if i >= len(node.Key) ||
		options.targetLayer != options.currentHeight {
		return false, nil
	}
	cmp, err := m.keyOrder(node.Key[i], k)
	if err != nil {
		return false, fmt.Errorf("keyCompare: %w", err)
	}
	if cmp != 0 {
		return false, nil
	}
	if value != nil {
		if node.Value[i] == nil {
			return true, nil
		}
		reflect.ValueOf(value).Elem().Set(reflect.ValueOf(node.Value[i]))
	}
	return true, nil
}

// Insert adds or replaces the value for the given key.
func (m *Mast) Insert(ctx context.Context, key interface{}, value interface{}) error {
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
	node, err := m.load(ctx, m.root)
	if err != nil {
		return err
	}
	node, i, err := node.findNode(ctx, m, key, &options)
	if err != nil {
		return err
	}
	if options.targetLayer != options.currentHeight {
		panic("dunno why we didn't land in the right layer")
	}
	if i < len(node.Key) {
		cmp, err := m.keyOrder(node.Key[i], key)
		if err != nil {
			return fmt.Errorf("keyCompare: %w", err)
		}
		if cmp == 0 {
			if node.Value[i] == value {
				return nil
			}
			node = node.ToMut(ctx, m)
			node.Dirty()
			node.Value[i] = value
			options.path[len(options.path)-1].node = node
			return m.savePathForRoot(ctx, options.path)
		}
	}
	// XXX do after split, XXX mark tree invalid if split fails
	node = node.ToMut(ctx, m)
	node.Dirty()
	if i < len(node.Key) {
		node.Key = append(node.Key[:i+1], node.Key[i:]...)
		node.Key[i] = key
		node.Value = append(node.Value[:i+1], node.Value[i:]...)
		node.Value[i] = value
	} else {
		node.Key = append(node.Key, key)
		node.Value = append(node.Value, value)
	}
	if i < len(node.Link) {
		node.Link = append(node.Link[:i+1], node.Link[i:]...)
	} else {
		node.Link = append(node.Link, nil)
	}
	var leftLink interface{}
	var rightLink interface{}
	if node.Link[i] != nil {
		child, err := m.load(ctx, node.Link[i])
		if err != nil {
			return err
		}
		if m.debug {
			fmt.Printf("  doing a split, of node with keys %v\n", child.Key)
		}
		leftLink, rightLink, err = split(ctx, child, key, m)
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
	err = m.savePathForRoot(ctx, options.path)
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
			m.dump(ctx)
		}
		err = m.grow(ctx)
		if err != nil {
			return fmt.Errorf("grow: %w", err)
		}
	}
	m.size++
	return nil
}

// Iter iterates over the entries of a tree, invoking the given callback for every entry's key and value.
func (m *Mast) Iter(ctx context.Context, f func(interface{}, interface{}) error) error {
	node, err := m.load(ctx, m.root)
	if err != nil {
		return err
	}
	return node.iter(ctx, f, m)
}

// keys returns the keys of the tree's entries as an array.
func (m *Mast) keys(ctx context.Context) ([]interface{}, error) {
	array := make([]interface{}, m.size)
	i := 0
	err := m.Iter(ctx, func(key interface{}, _ interface{}) error {
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
func (r *Root) LoadMast(ctx context.Context, config *RemoteConfig) (*Mast, error) {
	var link interface{}
	if r.Link != nil {
		link = *r.Link
	} else {
		link = emptyNodePointer(int(r.BranchFactor))
	}
	shrinkSize := uint64(1)
	for i := 0; i < int(r.Height); i++ {
		shrinkSize *= uint64(r.BranchFactor)
	}
	m := Mast{
		root:                           link,
		zeroKey:                        config.KeysLike,
		zeroValue:                      config.ValuesLike,
		unmarshal:                      config.Unmarshal,
		marshal:                        config.Marshal,
		unmarshalerUsesRegisteredTypes: config.UnmarshalerUsesRegisteredTypes,
		keyOrder:                       defaultOrder,
		keyLayer:                       defaultLayer,
		branchFactor:                   r.BranchFactor,
		size:                           r.Size,
		height:                         r.Height,
		persist:                        config.StoreImmutablePartsWith,
		shrinkBelowSize:                shrinkSize,
		growAfterSize:                  shrinkSize * uint64(r.BranchFactor),
		nodeCache:                      config.NodeCache,
	}
	if config.Unmarshal == nil {
		m.unmarshal = defaultUnmarshal
	}
	if config.Marshal == nil {
		m.marshal = defaultMarshal
	}
	err := m.checkRoot(ctx)
	if err != nil {
		return nil, fmt.Errorf("checkRoot: %w", err)
	}
	return &m, nil
}

// MakeRoot makes a new persistent root, after ensuring all the changed nodes
// have been written to the persistent store.
func (m *Mast) MakeRoot(ctx context.Context) (*Root, error) {
	link, err := m.flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}
	return &Root{&link, m.size, m.height, m.branchFactor}, nil
}

// NewInMemory returns a new tree for use as an in-memory data structure
// (i.e. that isn't intended to be remotely persisted).
func NewInMemory() Mast {
	return Mast{
		root:            emptyNodePointer(DefaultBranchFactor),
		branchFactor:    DefaultBranchFactor,
		growAfterSize:   DefaultBranchFactor,
		shrinkBelowSize: uint64(1),
		keyOrder:        defaultOrder,
		keyLayer:        defaultLayer,
		unmarshal:       defaultUnmarshal,
		marshal:         defaultMarshal,
	}
}

// NewRoot creates an empty tree whose nodes will be persisted remotely according to remoteOptions.
func NewRoot( /*config RemoteConfig,*/ remoteOptions *CreateRemoteOptions) *Root {
	branchFactor := uint(DefaultBranchFactor)
	if remoteOptions != nil && remoteOptions.BranchFactor > 0 {
		branchFactor = remoteOptions.BranchFactor
	}
	return &Root{nil, 0, 0, branchFactor}
}

// Height returns the number of levels between the leaves and root.
func (m *Mast) Height() uint8 {
	return m.height
}

// Size returns the number of entries in the tree.
func (m *Mast) Size() uint64 {
	return m.size
}

// toSlice returns an array of the tree's entries.
func (m *Mast) toSlice(ctx context.Context) ([]entry, error) {
	array := make([]entry, m.size)
	i := 0
	err := m.Iter(ctx, func(key interface{}, value interface{}) error {
		array[i] = entry{key, value}
		i++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return array, nil
}

func (m *Mast) Clone(ctx context.Context) (Mast, error) {
	newNode, err := m.load(ctx, m.root)
	if err != nil {
		return Mast{}, err
	}
	m2 := *m
	newRoot, err := newNode.ToShared()
	if err != nil {
		return Mast{}, err
	}
	m2.root = newRoot
	return m2, nil
}

// IsDirty signifies that in-memory values have been Set() or merged that haven't been Save()d.
func (m *Mast) IsDirty() bool {
	if node, ok := m.root.(*mastNode); ok {
		return node.dirty
	}
	return false
}
