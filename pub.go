package mast

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

var (
	// ErrIterDone is the error returned by Iter and SeekIter to stop the iteration
	ErrIterDone = errors.New("iter done")
)

// CreateRemoteOptions sets initial parameters for the tree, that would be painful to change after the tree has data.
type CreateRemoteOptions struct {
	// BranchFactor, or number of entries per node.  0 means use DefaultBranchFactor.
	BranchFactor uint
	// NodeFormat, defaults to more-compact "v1.1.5binary" for new trees, can be set to "v1marshaler" to make nodes compatible with pre-v1.1.5 code.
	NodeFormat nodeFormat
}
type nodeFormat string

var (
	V1Marshaler = nodeFormat("v1marshaler")
	V115Binary  = nodeFormat("v1.1.5binary")
)

// entry represents a key and value in the tree.
type entry struct {
	Key   interface{}
	Value interface{}
}

// Persist is the interface for loading and storing (serialized) tree nodes. The given string
// identity corresponds to the content which is immutable (never modified).
type Persist interface {
	// Store makes the given bytes accessible by the given name. The given string identity corresponds
	// to the content which is immutable (never modified).
	Store(context.Context, string, []byte) error
	// Load retrieves the previously-stored bytes by the given name.
	Load(context.Context, string) ([]byte, error)
	// NodeURLPrefix returns some string that identifies the
	// container this Persist uses, to enable NodeCaches to
	// distinguish identical nodes on different servers.
	NodeURLPrefix() string
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

	KeyCompare func(_, _ interface{}) (int, error)
}

// Root identifies a version of a tree whose nodes are accessible in the persistent store.
type Root struct {
	Link         *string
	Size         uint64
	Height       uint8
	BranchFactor uint
	NodeFormat   string `json:"NodeFormat,omitempty"`
}

// Delete deletes the entry with given key and value from the tree.
func (m *Mast) Delete(ctx context.Context, key, value interface{}) error {
	if m.debug {
		fmt.Printf("deleting %v...\n", key)
	}
	if m.root == nil {
		return fmt.Errorf("key %v not present in tree", key)
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
	node, i, err := findEntry(ctx, m, key, value, &options)
	if err != nil {
		return err
	}
	node, err = deleteEntry(ctx, m, node, i)
	if err != nil {
		return err
	}
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

func findEntry(ctx context.Context, m *Mast, key, value interface{}, options *findOptions) (*mastNode, int, error) {
	node, err := m.load(ctx, m.root)
	if err != nil {
		return nil, 0, fmt.Errorf("load root: %w", err)
	}
	node, i, err := node.findNode(ctx, m, key, options)
	if err != nil {
		return nil, 0, fmt.Errorf("findNode: %w", err)
	}
	if options.targetLayer != options.currentHeight ||
		i == len(node.Key) {
		return nil, 0, fmt.Errorf("key %v not present in tree", key)
	}
	cmp, err := m.keyOrder(node.Key[i], key)
	if err != nil {
		return nil, 0, fmt.Errorf("keyCompare: %w", err)
	}
	if cmp != 0 {
		return nil, 0, fmt.Errorf("key %v not present in tree", key)
	}
	if node.Value[i] != value {
		return nil, 0, fmt.Errorf("value not present for given key (found=%v, wanted=%v)", node.Value[i], value)
	}
	return node, i, nil
}

func deleteEntry(ctx context.Context, m *Mast, node *mastNode, i int) (*mastNode, error) {
	oldNode := node
	mergedLink, err := m.mergeNodes(ctx, oldNode.Link[i], oldNode.Link[i+1])
	if err != nil {
		return nil, fmt.Errorf("merge: %w", err)
	}
	node = node.ToMut(ctx, m)
	node.Dirty()
	node.Key = append(node.Key[:i], node.Key[i+1:]...)
	node.Value = append(node.Value[:i], node.Value[i+1:]...)
	node.Link = append(node.Link[:i], node.Link[i+1:]...)
	node.Link[i] = mergedLink
	return node, nil
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

// DiffLinks invokes the given callback for every node that is different from the given tree. The
// iteration will stop if the callback returns keepGoing==false or an error.
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
	if m.root == nil {
		return "", nil
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
				cberr := f()
				if cberr != nil {
					seLock.Lock()
					if firstStoreError == nil {
						firstStoreError = cberr
					}
					seLock.Unlock()
				}
			}()
		}
		wg.Done()
	}()

	if !m.unmarshalerUsesRegisteredTypes && (m.zeroKey == nil || m.zeroValue == nil) {
		return "", errors.New("will not be able to figure out which type to unmarshal entries as; set RemoteConfig.{Keys,Values}Like or UnmarshalerUsesRegisteredTypes")
	}

	versionedMarshaler := func(i interface{}) ([]byte, error) {
		switch m.nodeFormat {
		case V1Marshaler:
			switch x := i.(type) {
			case mastNode:
				return m.marshal(x.Node)
			default:
				return m.marshal(x)
			}
		case V115Binary:
			node, ok := i.(mastNode)
			if !ok {
				return nil, fmt.Errorf("expected mast.mastNode, got %T", i)
			}
			return marshalMastNode(&node, m.marshal)
		}
		return nil, fmt.Errorf("unknown node format '%v'", m.nodeFormat)
	}

	str, err := node.store(ctx, m.persist, m.nodeCache, versionedMarshaler, storeQ)
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

// Get gets the value of the entry with the given key and stores it at the given value pointer.
// Returns false if the tree doesn't contain the given key.
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
func (m *Mast) Insert(ctx context.Context, key, value interface{}) error {
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
	var node *mastNode
	var i int
	if m.root == nil {
		node = emptyNodePointer(int(m.branchFactor))
	} else {
		node, err = m.load(ctx, m.root)
		if err != nil {
			return err
		}
	}
	node, i, err = node.findNode(ctx, m, key, &options)
	if err != nil {
		return err
	}
	if options.targetLayer != options.currentHeight {
		panic("dunno why we didn't land in the right layer")
	}
	if i < len(node.Key) {
		var cmp int
		cmp, err = m.keyOrder(node.Key[i], key)
		if err != nil {
			return fmt.Errorf("keyCompare: %w", err)
		}
		if cmp == 0 {
			if reflect.DeepEqual(node.Value[i], value) {
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
		var child *mastNode
		child, err = m.load(ctx, node.Link[i])
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
	err = node.iter(ctx, f, m)
	if err == nil || err == ErrIterDone {
		return nil
	}
	return err
}

// Seekiter is similar to Iter, but the difference is to find the first position greater than or equal to the key and start the iteration
func (m *Mast) SeekIter(ctx context.Context, k interface{}, f func(interface{}, interface{}) error) error {
	if m.root == nil {
		return nil
	}
	node, err := m.load(ctx, m.root)
	if err != nil {
		return err
	}
	keyLayer, err := m.keyLayer(k, m.branchFactor)
	if err != nil {
		return fmt.Errorf("layer: %w", err)
	}
	options := findOptions{
		targetLayer:   uint8min(keyLayer, m.height),
		currentHeight: m.height,
	}
	node, i, err := node.findNode(ctx, m, k, &options)
	if err != nil {
		return err
	}
	if i >= len(node.Key) ||
		options.targetLayer != options.currentHeight {
		return nil
	}
	for i := len(options.path) - 1; i >= 0; i-- {
		entry := options.path[i]
		err = entry.node.seekIter(ctx, entry.linkIndex, f, m)
		if err == ErrIterDone {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
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
	var nf nodeFormat
	switch r.NodeFormat {
	case string(V115Binary):
		nf = V115Binary
	case "", string(V1Marshaler):
		nf = V1Marshaler
	default:
		return nil, fmt.Errorf("unknown node format: %s", r.NodeFormat)
	}

	m := Mast{
		root:                           link,
		zeroKey:                        config.KeysLike,
		zeroValue:                      config.ValuesLike,
		unmarshal:                      config.Unmarshal,
		marshal:                        config.Marshal,
		unmarshalerUsesRegisteredTypes: config.UnmarshalerUsesRegisteredTypes,
		keyOrder:                       config.KeyCompare,
		branchFactor:                   r.BranchFactor,
		size:                           r.Size,
		height:                         r.Height,
		persist:                        config.StoreImmutablePartsWith,
		shrinkBelowSize:                shrinkSize,
		growAfterSize:                  shrinkSize * uint64(r.BranchFactor),
		nodeCache:                      config.NodeCache,
		nodeFormat:                     nf,
	}
	if config.Unmarshal == nil {
		m.unmarshal = defaultUnmarshal
	}
	if config.Marshal == nil {
		m.marshal = defaultMarshal
	}
	if config.KeyCompare == nil {
		m.keyOrder = DefaultKeyCompare(m.marshal)
	}
	m.keyLayer = DefaultLayer(m.marshal)
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
	linkp := &link
	if link == "" {
		linkp = nil
	}
	return &Root{
		Link:         linkp,
		Size:         m.size,
		Height:       m.height,
		BranchFactor: m.branchFactor,
		NodeFormat:   string(m.nodeFormat),
	}, nil
}

// NewInMemory returns a new tree for use as an in-memory data structure
// (i.e. that isn't intended to be remotely persisted).
func NewInMemory() Mast {
	return Mast{
		root:            emptyNodePointer(DefaultBranchFactor),
		branchFactor:    DefaultBranchFactor,
		growAfterSize:   DefaultBranchFactor,
		shrinkBelowSize: uint64(1),
		keyOrder:        DefaultKeyCompare(defaultMarshal),
		keyLayer:        DefaultLayer(defaultMarshal),
		unmarshal:       defaultUnmarshal,
		marshal:         defaultMarshal,
	}
}

// NewRoot creates an empty tree whose nodes will be persisted remotely according to remoteOptions.
func NewRoot( /*config RemoteConfig,*/ remoteOptions *CreateRemoteOptions) *Root {
	branchFactor := uint(DefaultBranchFactor)
	nf := V115Binary
	if remoteOptions != nil {
		if remoteOptions.BranchFactor > 0 {
			branchFactor = remoteOptions.BranchFactor
		}
		if remoteOptions.NodeFormat != nodeFormat("") {
			nf = remoteOptions.NodeFormat
		}
	}
	return &Root{
		Link:         nil,
		Size:         0,
		Height:       0,
		BranchFactor: branchFactor,
		NodeFormat:   string(nf),
	}
}

// Height returns the number of levels between the leaves and root.
func (m *Mast) Height() uint8 {
	return m.height
}

// Size returns the number of entries in the tree.
func (m *Mast) Size() uint64 {
	return m.size
}

// Clone() returns a new Mast that shares all the source's data
// but can evolve independently (copy-on-write).
func (m *Mast) Clone(ctx context.Context) (Mast, error) {
	m2 := *m
	if m.root != nil {
		newNode, err := m.load(ctx, m.root)
		if err != nil {
			return Mast{}, err
		}
		newRoot, err := newNode.ToShared()
		if err != nil {
			return Mast{}, err
		}
		m2.root = newRoot
	}
	return m2, nil
}

// IsDirty signifies that in-memory values have been Set() or merged that haven't been Save()d.
func (m *Mast) IsDirty() bool {
	if node, ok := m.root.(*mastNode); ok {
		return node.dirty
	}
	return false
}

// Cursor can be used to seek around a tree.
type Cursor struct {
	path []pathEntry
	m    *Mast
}

// Cursor obtains a cursor set to the smallest value in the root node.
func (m *Mast) Cursor(ctx context.Context) (*Cursor, error) {
	nm, err := m.Clone(ctx)
	if err != nil {
		return nil, fmt.Errorf("clone: %w", err)
	}
	m = &nm
	if m.root == nil {
		return &Cursor{
			m:    m,
			path: nil,
		}, nil
	}
	node, err := m.load(ctx, m.root)
	if err != nil {
		return nil, fmt.Errorf("load root: %w", err)
	}
	cursor := &Cursor{
		m: m,
		path: []pathEntry{
			{node, 0},
		},
	}
	return cursor, nil
}

// Min moves the cursor to the smallest key in the subtree under the current position.
func (c *Cursor) Min(ctx context.Context) error {
	if len(c.path) == 0 {
		return nil
	}
	pe := c.path[len(c.path)-1]
	node := pe.node
	for {
		if len(node.Link) == 0 || node.Link[0] == nil {
			return nil
		}
		child, err := node.follow(ctx, 0, false, c.m)
		if err != nil {
			return fmt.Errorf("following %d: %w", 0, err)
		}
		if child == node {
			return nil
		}
		node = child
		c.path = append(c.path,
			pathEntry{node, 0})
	}
}

// Max moves the cursor to the largest key in the subtree under the current position.
func (c *Cursor) Max(ctx context.Context) error {
	if len(c.path) == 0 {
		return nil
	}
	pe := c.path[len(c.path)-1]
	node := pe.node
	c.path = c.path[:len(c.path)-1]
	for {
		if len(node.Link) == 0 || node.Link[len(node.Link)-1] == nil {
			c.path = append(c.path,
				pathEntry{node, len(node.Value) - 1})
			return nil
		} else {
			c.path = append(c.path,
				pathEntry{node, len(node.Link) - 1})
		}
		child, err := node.follow(ctx, len(node.Link)-1, false, c.m)
		if err != nil {
			return fmt.Errorf("following %d: %w", 0, err)
		}
		if child == node {
			return nil
		}
		node = child
	}
}

// Get returns the key and value of the entry at the cursor, if there is an entry,
// or !ok if there is no entry.
func (c *Cursor) Get() (interface{}, interface{}, bool) {
	if len(c.path) == 0 {
		return nil, nil, false
	}
	pe := c.path[len(c.path)-1]
	node := pe.node
	if pe.linkIndex >= len(node.Key) {
		return nil, nil, false
	}
	return node.Key[pe.linkIndex], node.Value[pe.linkIndex], true
}

// Forward moves the cursor to the entry with the next-larger key.
func (c *Cursor) Forward(ctx context.Context) error {
	if len(c.path) == 0 {
		return nil
	}
	pe := &c.path[len(c.path)-1]
	node := pe.node
	if pe.linkIndex+1 < len(node.Link) && node.Link[pe.linkIndex+1] != nil {
		node, err := c.m.load(ctx, pe.node.Link[pe.linkIndex+1])
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}
		pe.linkIndex++
		c.path = append(c.path, pathEntry{node: node})
		return c.Min(ctx)
	} else {
		if pe.linkIndex+1 < len(node.Key) {
			pe.linkIndex++
			return nil
		}
		for {
			c.path = c.path[:len(c.path)-1]
			if len(c.path) == 0 {
				return nil
			}
			pe = &c.path[len(c.path)-1]
			if pe.linkIndex < len(pe.node.Key) {
				return nil
			}
		}
	}
}

// Backward moves the cursor to the entry with th enext-smaller key.
func (c *Cursor) Backward(ctx context.Context) error {
	if len(c.path) == 0 {
		return nil
	}
	pe := &c.path[len(c.path)-1]
	node := pe.node
	if node.Link[0] != nil {
		node, err := c.m.load(ctx, pe.node.Link[pe.linkIndex])
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}
		c.path = append(c.path, pathEntry{node: node})
		return c.Max(ctx)
	} else {
		if pe.linkIndex > 0 {
			pe.linkIndex--
			return nil
		}
		for {
			c.path = c.path[:len(c.path)-1]
			if len(c.path) == 0 {
				return nil
			}
			pe = &c.path[len(c.path)-1]
			if pe.linkIndex > 0 {
				pe.linkIndex--
				return nil
			}
		}
	}
}

// search1 updates the current path's linkIndex to the subtree
// which would contain the given key.
func (c *Cursor) search1(ctx context.Context, key interface{}) error {
	pe := &c.path[len(c.path)-1]
	node := pe.node
	i := len(node.Key)
	var err error
	cmp := -1
	if i > 0 {
		// check max first, optimizing for in-order insertion
		cmp, err = c.m.keyOrder(key, node.Key[i-1])
		if err != nil {
			return fmt.Errorf("keyCompare: %w", err)
		}
		if cmp <= 0 {
			i--
		}
	}
	if cmp < 0 {
		i = sort.Search(i, func(i int) bool {
			if err != nil {
				return true
			}
			cmp, err = c.m.keyOrder(key, node.Key[i])
			if err != nil {
				err = fmt.Errorf("keyCompare: %w", err)
				return true
			}
			return cmp <= 0
		})
	}
	pe.linkIndex = i
	return nil
}

// Ceil moves the cursor to the entry with the given key, or if not present,
// the entry with the next-larger key.
func (c *Cursor) Ceil(ctx context.Context, key interface{}) error {
	for {
		err := c.search1(ctx, key)
		if err != nil {
			return err
		}
		pe := &c.path[len(c.path)-1]
		node := pe.node
		if pe.linkIndex < len(node.Key) {
			cmp, err := c.m.keyOrder(key, node.Key[pe.linkIndex])
			if err != nil {
				return fmt.Errorf("keyOrder: %w", err)
			}
			if cmp == 0 {
				return nil
			}
		}
		if node.Link[pe.linkIndex] == nil {
			// exhausted left subtree; go up to ceil
			for pe.linkIndex == len(node.Key) {
				c.path = c.path[:len(c.path)-1]
				if len(c.path) == 0 {
					return nil
				}
				pe = &c.path[len(c.path)-1]
				node = pe.node
			}
			return nil
		}
		node, err = c.m.load(ctx, pe.node.Link[pe.linkIndex])
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}
		c.path = append(c.path, pathEntry{node: node})
	}
}

func (c *Cursor) String() string {
	res := ""
	for i := range c.path {
		var ks string
		if c.path[i].linkIndex < len(c.path[i].node.Key) {
			ks = fmt.Sprintf("%v", c.path[i].node.Key[c.path[i].linkIndex])
		} else {
			ks = fmt.Sprintf(">%v", c.path[i].node.Key[c.path[i].linkIndex-1])
		}
		if i > 0 {
			res += " "
		}
		res += fmt.Sprintf("%v (index=%d)", ks, c.path[i].linkIndex)
	}
	return res
}
