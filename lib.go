package mast

import (
	"context"
	"fmt"
	"reflect"
	"sort"
)

// DefaultBranchFactor is how many entries per node a tree will normally have.
const DefaultBranchFactor = 16

// Mast encapsulates data and parameters for the in-memory portion of a Merkle Search Tree.
type Mast struct {
	root                           interface{}
	zeroKey                        interface{}
	zeroValue                      interface{}
	keyOrder                       func(_, _ interface{}) (int, error)
	keyLayer                       func(key interface{}, branchFactor uint) (uint8, error)
	unmarshalerUsesRegisteredTypes bool
	marshal                        func(interface{}) ([]byte, error)
	unmarshal                      func([]byte, interface{}) error
	branchFactor                   uint
	height                         uint8
	size                           uint64
	growAfterSize                  uint64
	shrinkBelowSize                uint64
	persist                        Persist
	debug                          bool
	nodeCache                      NodeCache
	nodeFormat                     nodeFormat
}

type mastNode struct {
	Key      []interface{}
	Value    []interface{}
	Link     []interface{} `json:",omitempty"`
	dirty    bool
	shared   bool
	expected *mastNode
	source   *string
}

type pathEntry struct {
	node      *mastNode
	linkIndex int
}

func (m *Mast) savePathForRoot(ctx context.Context, path []pathEntry) error {
	for i := 0; i < len(path); i++ {
		if !path[i].node.dirty {
			path[i].node = path[i].node.ToMut(ctx, m)
			path[i].node.dirty = true
			path[i].node.expected = nil
			path[i].node.source = nil
		}
	}
	for i := len(path) - 2; i >= 0; i-- {
		entry := path[i]
		if !path[i+1].node.isEmpty() {
			entry.node.Link[entry.linkIndex] = path[i+1].node
		} else {
			entry.node.Link[entry.linkIndex] = nil
		}
	}
	if !path[0].node.isEmpty() {
		m.root = path[0].node
	} else {
		m.root = nil
	}
	return nil
}

// Splits the given node into two: left and right, so they could be the left+right children of a
// parent entry with the given key. The key is not expected to already be present in the source
// node, and will panic--but it would not migrated to the output, so that the caller can decide
// where to put it and its new children.
func split(ctx context.Context, node *mastNode, key interface{}, mast *Mast) (leftLink, rightLink interface{}, err error) {
	var splitIndex int
	for splitIndex = 0; splitIndex < len(node.Key); splitIndex++ {
		var cmp int
		cmp, err = mast.keyOrder(node.Key[splitIndex], key)
		if err != nil {
			return nil, nil, fmt.Errorf("keyCompare: %w", err)
		}
		if cmp == 0 {
			panic("split shouldn't need to handle preservation of already-present key")
		}
		if cmp > 0 {
			break
		}
	}
	var tooBigLink interface{} = nil
	left := mastNode{
		make([]interface{}, 0, cap(node.Key)),
		make([]interface{}, 0, cap(node.Value)),
		make([]interface{}, 0, cap(node.Link)),
		true, false, nil, nil,
	}
	left.Key = append(left.Key, node.Key[:splitIndex]...)
	left.Value = append(left.Value, node.Value[:splitIndex]...)
	left.Link = append(left.Link, node.Link[:splitIndex+1]...)

	// repartition the left and right subtrees based on the new key
	leftMaxLink := left.Link[len(left.Link)-1]
	if leftMaxLink != nil {
		var leftMax *mastNode
		leftMax, err = mast.load(ctx, leftMaxLink)
		if mast.debug {
			fmt.Printf("  splitting leftMax, node with keys: %v\n", leftMax.Key)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("loading leftMax: %w", err)
		}
		leftMaxLink, tooBigLink, err = split(ctx, leftMax, key, mast)
		if err != nil {
			return nil, nil, fmt.Errorf("splitting leftMax: %w", err)
		}
		if mast.debug {
			fmt.Printf("  splitting leftMax, node with keys: %v is done: leftMaxLink=%v, tooBigLink=%v\n", leftMax.Key, leftMaxLink, tooBigLink)
		}
		left.Link[len(left.Link)-1] = leftMaxLink
	}
	if !left.isEmpty() {
		leftLink, err = mast.store(&left)
		if err != nil {
			return nil, nil, fmt.Errorf("store left: %w", err)
		}
	}
	right := mastNode{
		make([]interface{}, 0, cap(node.Key)),
		make([]interface{}, 0, cap(node.Value)),
		make([]interface{}, 0, cap(node.Link)),
		true, false, nil, nil,
	}
	right.Key = append(right.Key, node.Key[splitIndex:]...)
	right.Value = append(right.Value, node.Value[splitIndex:]...)
	right.Link = append(right.Link, node.Link[splitIndex:]...)
	right.Link[0] = tooBigLink

	rightMinLink := right.Link[0]
	if rightMinLink != nil {
		var rightMin *mastNode
		rightMin, err = mast.load(ctx, rightMinLink)
		if err != nil {
			return nil, nil, fmt.Errorf("load rightMin: %w", err)
		}
		var tooSmallLink interface{}
		tooSmallLink, rightMinLink, err = split(ctx, rightMin, key, mast)
		if err != nil {
			return nil, nil, fmt.Errorf("split rightMin: %w", err)
		}
		if mast.debug {
			fmt.Printf("  splitting rightMin, node with keys %v, is done: tooSmallLink=%v, rightMinLink=%v",
				rightMin.Key, tooSmallLink, rightMinLink)
		}
		right.Link[0] = rightMinLink
		if tooSmallLink != nil {
			panic("inconsistent node order: non-nil tooSmall")
		}
	}
	if !right.isEmpty() {
		rightLink, err = mast.store(&right)
		if err != nil {
			return nil, nil, err
		}
	}
	// TODO: common case maybe not dirty
	node.dirty = true
	node.expected = nil
	node.source = nil
	return leftLink, rightLink, nil
}

func (node *mastNode) isEmpty() bool {
	return len(node.Link) == 1 && node.Link[0] == nil
}

type findOptions struct {
	targetLayer        uint8
	currentHeight      uint8
	createMissingNodes bool
	path               []pathEntry
}

func (node *mastNode) findNode(ctx context.Context, m *Mast, key interface{}, options *findOptions) (*mastNode, int, error) {
	i := len(node.Key)
	if len(node.Link) != i+1 {
		node.dump(ctx, m)
		panic(fmt.Sprintf("node %p doesn't have N+1 links", node))
	}
	var err error
	cmp := -1
	if i > 0 {
		// check max first, optimizing for in-order insertion
		cmp, err = m.keyOrder(key, node.Key[i-1])
		if err != nil {
			return nil, 0, fmt.Errorf("keyCompare: %w", err)
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
			cmp, err = m.keyOrder(key, node.Key[i])
			if err != nil {
				err = fmt.Errorf("keyCompare: %w", err)
				return true
			}
			return cmp <= 0
		})
	}
	options.path = append(options.path,
		pathEntry{node, i})
	if cmp == 0 || options.currentHeight == options.targetLayer {
		return node, i, nil
	}

	child, err := node.follow(ctx, i, options.createMissingNodes, m)
	if err != nil {
		return nil, 0, fmt.Errorf("following %d: %w", i, err)
	}
	options.currentHeight--
	return child.findNode(ctx, m, key, options)
}

func (node *mastNode) follow(ctx context.Context, i int, createOk bool, mast *Mast) (*mastNode, error) {
	if node.Link[i] != nil {
		child, err := mast.load(ctx, node.Link[i])
		if err != nil {
			return nil, fmt.Errorf("follow load %v: %w", node.Link[i], err)
		}
		return child, nil
	} else if !createOk {
		return node, nil
	} else {
		child := emptyNodePointer(cap(node.Key))
		node.Link[i] = child
		return child, nil
	}
}

func uint8min(x, y uint8) uint8 {
	if x < y {
		return x
	}
	return y
}

func emptyNode(branchFactor int) mastNode {
	newNode := mastNode{
		Key:   make([]interface{}, 0, branchFactor),
		Value: make([]interface{}, 0, branchFactor),
		Link:  make([]interface{}, 1, branchFactor+1),
	}
	newNode.Link[0] = nil
	return newNode
}

func emptyNodePointer(branchFactor int) *mastNode {
	node := emptyNode(branchFactor)
	return &node
}

func (node *mastNode) extract(from, to int) *mastNode {
	newChild := emptyNode(cap(node.Key))
	newChild.Key = append([]interface{}{}, node.Key[from:to]...)
	newChild.Value = append([]interface{}{}, node.Value[from:to]...)
	newChild.Link = append([]interface{}{}, node.Link[from:to+1]...)
	if len(newChild.Key) != len(newChild.Value) {
		panic("keys and values not same length")
	}
	if len(newChild.Link) != len(newChild.Key)+1 {
		panic("links is not expected length")
	}
	if newChild.isEmpty() {
		return nil
	}
	newChild.dirty = true
	newChild.expected = nil
	newChild.source = nil
	return &newChild
}

func (m *Mast) grow(ctx context.Context) error {
	var node *mastNode
	var err error
	if m.debug {
		fmt.Printf("GROWING\n")
	}
	node, err = m.load(ctx, m.root)
	if err != nil {
		return fmt.Errorf("load root: %w", err)
	}
	newNode := emptyNode(int(m.branchFactor))
	start := 0
	for i, key := range node.Key {
		var layer uint8
		layer, err = m.keyLayer(key, m.branchFactor)
		if err != nil {
			return fmt.Errorf("layer: %w", err)
		}
		if layer <= m.height {
			continue
		}
		newLeftNode := node.extract(start, i)
		if m.debug {
			fmt.Printf("  extracted left: %v\n", node)
		}
		var newLeftLink interface{}
		if newLeftNode != nil {
			newLeftLink, err = m.store(newLeftNode)
			if err != nil {
				return err
			}
		} else {
			newLeftLink = nil
		}
		newNode.Key = append(newNode.Key, key)
		newNode.Value = append(newNode.Value, node.Value[i])
		newNode.Link[len(newNode.Link)-1] = newLeftLink
		newNode.Link = append(newNode.Link, nil)
		if len(newNode.Link) != len(newNode.Key)+1 {
			panic("new node has wrong number of links")
		}
		start = i + 1
	}
	newRightNode := node.extract(start, len(node.Key))
	if newRightNode != nil {
		if m.debug {
			fmt.Printf("extracted right:\n")
			newRightNode.dump(ctx, m)
		}
		var newRightLink interface{}
		newRightLink, err = m.store(newRightNode)
		if err != nil {
			return err
		}
		newNode.Link[len(newNode.Link)-1] = newRightLink
	}
	newNode.dirty = true
	newNode.expected = nil
	newNode.source = nil
	newLink, err := m.store(&newNode)
	if err != nil {
		return err
	}
	m.root = newLink
	m.height++
	m.shrinkBelowSize = m.growAfterSize
	m.growAfterSize *= uint64(m.branchFactor)
	return nil
}

func (node *mastNode) canGrow(currentHeight uint8, keyLayer func(interface{}, uint) (uint8, error), branchFactor uint) (bool, error) {
	for _, key := range node.Key {
		layer, err := keyLayer(key, branchFactor)
		if err != nil {
			return false, fmt.Errorf("layer: %w", err)
		}
		if layer > currentHeight {
			return true, nil
		}
	}
	return false, nil
}

func (m *Mast) shrink(ctx context.Context) error {
	var err error
	if m.debug {
		fmt.Printf("SHRINKING\n")
		fmt.Printf("size=%d height=%d branchFactor=%d\n", m.size, m.height, m.branchFactor)
		m.dump(ctx)
	}
	if m.height == 0 {
		return fmt.Errorf("tree is too short to shrink")
	}
	if m.root == nil {
		if m.height != 0 {
			return fmt.Errorf("tree with empty root but height %d, size %d", m.height, m.size)
		}
		return nil
	}
	node, err := m.load(ctx, m.root)
	if err != nil {
		return fmt.Errorf("load root: %w", err)
	}
	newNode := mastNode{
		Key:   make([]interface{}, 0, m.branchFactor),
		Value: make([]interface{}, 0, m.branchFactor),
		Link:  make([]interface{}, 0, m.branchFactor+1),
		dirty: true,
	}
	for i := range node.Link {
		if node.Link[i] != nil {
			child, err := m.load(ctx, node.Link[i])
			if err != nil {
				return fmt.Errorf("load child: %w", err)
			}
			newNode.Key = append(newNode.Key, child.Key...)
			newNode.Value = append(newNode.Value, child.Value...)
			newNode.Link = append(newNode.Link, child.Link...)
			validateNode(ctx, &newNode, m)
		} else {
			newNode.Link = append(newNode.Link, nil)
		}
		if i < len(node.Key) {
			newNode.Key = append(newNode.Key, node.Key[i])
			newNode.Value = append(newNode.Value, node.Value[i])
		}
	}
	validateNode(ctx, &newNode, m)
	if !newNode.isEmpty() {
		newLink, err := m.store(&newNode)
		if err != nil {
			return err
		}
		m.root = newLink
	} else {
		m.root = nil
	}
	m.height--
	if m.debug {
		fmt.Printf("after shrink:\n")
		fmt.Printf("size=%d height=%d branchFactor=%d\n", m.size, m.height, m.branchFactor)
		m.dump(ctx)
	}
	if m.shrinkBelowSize > 1 {
		m.shrinkBelowSize /= uint64(m.branchFactor)
		m.growAfterSize /= uint64(m.branchFactor)
	}
	return nil
}

func (node *mastNode) dump(ctx context.Context, mast *Mast) {
	str, err := node.string(ctx, "  ", mast)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", str)
}

func (node *mastNode) string(ctx context.Context, indent string, mast *Mast) (string, error) {
	res := ""
	for i := range node.Link {
		var label string
		if i >= len(node.Key) {
			label = ">"
		} else {
			label = fmt.Sprintf("%v: %v", node.Key[i], node.Value[i])
		}
		linkStr := ""
		if ls, ok := node.Link[i].(string); ok {
			linkStr = fmt.Sprintf(" link=%v", ls)
		}

		res += fmt.Sprintf("%s%s%s {", indent, label, linkStr)
		if node.Link[i] == nil {
			res += "}\n"
			continue
		}
		child, err := mast.load(ctx, node.Link[i])
		if err != nil {
			return "", err
		}
		res += "\n"
		childstr, err := child.string(ctx, indent+"   ", mast)
		if err != nil {
			return "", err
		}
		res += childstr
		res += indent + "}\n"
	}
	return res, nil
}

func (m *Mast) dump(ctx context.Context) {
	if m.root == nil {
		fmt.Printf("NIL\n")
		return
	}
	node, err := m.load(ctx, m.root)
	if err != nil {
		panic(err)
	}
	str, err := node.string(ctx, "   ", m)
	if err != nil {
		panic(err)
	}
	fmt.Printf("{\n%s}\n", str)
}

func (node *mastNode) iter(ctx context.Context, f func(interface{}, interface{}) error, mast *Mast) error {
	if mast.debug {
		fmt.Printf("starting iter at node with keys: %v\n", node.Key)
	}
	for i, link := range node.Link {
		if link != nil {
			child, err := mast.load(ctx, link)
			if err != nil {
				return err
			}
			err = child.iter(ctx, f, mast)
			if err != nil {
				return err
			}
		}
		if i < len(node.Key) {
			err := f(node.Key[i], node.Value[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// seekIter first seeks to idx key, and then starts the iteration
func (node *mastNode) seekIter(ctx context.Context, idx int, f func(interface{}, interface{}) error, m *Mast) error {
	if idx >= len(node.Key) {
		return nil
	}
	err := f(node.Key[idx], node.Value[idx])
	if err != nil {
		return err
	}
	for i := idx + 1; i < len(node.Link); i++ {
		link := node.Link[i]
		if link != nil {
			child, err := m.load(ctx, link)
			if err != nil {
				return err
			}
			err = child.iter(ctx, f, m)
			if err != nil {
				return err
			}
		}
		if i < len(node.Key) {
			err := f(node.Key[i], node.Value[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func validateNode(ctx context.Context, node *mastNode, mast *Mast) {
	if node.expected != nil {
		if !reflect.DeepEqual(node.expected.Key, node.Key) {
			fmt.Printf("expected node %v\n", node.expected)
			fmt.Printf("found    node %v\n", node)
			panic("nodes differ in keys")
		}
		if !reflect.DeepEqual(node.expected.Value, node.Value) {
			fmt.Printf("expected node %v\n", node.expected)
			fmt.Printf("found    node %v\n", node)
			panic("nodes differ in values")
		}
	}
	for i := 0; i < len(node.Key)-1; i++ {
		cmp, err := mast.keyOrder(node.Key[0], node.Key[1])
		if err != nil {
			panic(err)
		}
		if cmp >= 0 {
			panic(fmt.Sprintf("sweet merciful crap! %v >= %v!", node.Key[0], node.Key[1]))
		}
	}
	if len(node.Link) != len(node.Key)+1 {
		fmt.Println("DANGIT! {")
		node.dump(ctx, mast)
		fmt.Println("}")
		panic(fmt.Sprintf("node %p has %d links but %d keys", node, len(node.Link), len(node.Key)))
	}
	if len(node.Link) != len(node.Value)+1 {
		fmt.Println("DANGIT! {")
		node.dump(ctx, mast)
		fmt.Println("}")
		panic(fmt.Sprintf("node %p has %d links but %d values", node, len(node.Link), len(node.Value)))
	}
}

func (m *Mast) mergeNodes(ctx context.Context, leftLink, rightLink interface{}) (interface{}, error) {
	if leftLink == nil {
		return rightLink, nil
	}
	if rightLink == nil {
		return leftLink, nil
	}
	left, err := m.load(ctx, leftLink)
	if err != nil {
		return nil, fmt.Errorf("load left: %w", err)
	}
	right, err := m.load(ctx, rightLink)
	if err != nil {
		return nil, fmt.Errorf("load right: %w", err)
	}
	combined := &mastNode{
		Key:   make([]interface{}, 0, m.branchFactor),
		Value: make([]interface{}, 0, m.branchFactor),
		Link:  make([]interface{}, 0, m.branchFactor+1),
		dirty: true,
	}
	combined.Key = append(combined.Key, left.Key...)
	combined.Key = append(combined.Key, right.Key...)
	combined.Value = append(combined.Value, left.Value...)
	combined.Value = append(combined.Value, right.Value...)
	combined.Link = append(combined.Link, left.Link[0:len(left.Link)-1]...)
	combined.Link = append(combined.Link, nil)
	combined.Link = append(combined.Link, right.Link[1:]...)
	var mergedLink interface{}
	mergedLink, err = m.mergeNodes(ctx, left.Link[len(left.Link)-1], right.Link[0])
	if err != nil {
		return nil, fmt.Errorf("merge: %w", err)
	}
	combined.Link[len(left.Link)-1] = mergedLink
	var combinedLink interface{}
	combinedLink, err = m.store(combined)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	return combinedLink, nil
}

func (node *mastNode) xcopy() *mastNode {
	newNode := mastNode{
		make([]interface{}, 0, cap(node.Key)),
		make([]interface{}, 0, cap(node.Value)),
		make([]interface{}, 0, cap(node.Link)),
		node.dirty, node.shared, nil, nil,
	}
	newNode.Key = append(newNode.Key, node.Key...)
	newNode.Value = append(newNode.Value, node.Value...)
	newNode.Link = append(newNode.Link, node.Link...)
	return &newNode
}

func (m *Mast) checkRoot(ctx context.Context) error {
	node, err := m.load(ctx, m.root)
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	if len(node.Key) != len(node.Value) ||
		len(node.Link) != len(node.Key)+1 {
		return fmt.Errorf("impromperly-formatted node")
	}
	var last interface{}
	for i, key := range node.Key {
		if i > 0 {
			cmp, err := m.keyOrder(last, key)
			if err != nil {
				return fmt.Errorf("key order: %w", err)
			}
			if cmp >= 0 {
				return fmt.Errorf("inconsistent key order function; ensure using same function as source")
			}
		}
		layer, err := m.keyLayer(key, m.branchFactor)
		if err != nil {
			return fmt.Errorf("key layer: %w", err)
		}
		if layer < m.height {
			return fmt.Errorf("inconsistent key layers; ensure using same function as source")
		}
		last = key
	}
	return nil
}

func (node *mastNode) ToMut(ctx context.Context, mast *Mast) *mastNode {
	validateNode(ctx, node, mast)
	if !node.shared {
		return node
	}
	newNode := node.xcopy()
	newNode.expected = node
	newNode.shared = false
	return newNode
}

func (node *mastNode) ToShared() (*mastNode, error) {
	if node.shared {
		return node, nil
	}
	node = node.xcopy()
	var err error
	for i, link := range node.Link {
		switch l := link.(type) {
		case *mastNode:
			if l.shared {
				continue
			}
			node.Link[i], err = l.ToShared()
			if err != nil {
				return nil, err
			}
		case string:
		case nil:
		default:
			return nil, fmt.Errorf("unhandled link type %T", l)
		}
	}
	return node, nil
}

func (node *mastNode) Dirty() {
	node.expected = nil
	node.source = nil
}

// BranchFactor returns the ideal number of entries that are stored per node.
func (m *Mast) BranchFactor() uint {
	return m.branchFactor
}
