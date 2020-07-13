package mast

import (
	"fmt"
)

// DefaultBranchFactor is how many entries per node a tree will normally have.
const DefaultBranchFactor = 16

// Mast encapsulates data and parameters for the in-memory portion of a Merkle Search Tree.
type Mast struct {
	root                           interface{}
	zeroKey                        interface{}
	zeroValue                      interface{}
	keyOrder                       func(interface{}, interface{}) (int, error)
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
}

type mastNode struct {
	Key   []interface{}
	Value []interface{}
	Link  []interface{} `json:",omitempty"`
}

type pathEntry struct {
	node      *mastNode
	linkIndex int
}

func (m *Mast) savePathForRoot(path []pathEntry) error {
	for i := 0; i < len(path)-1; i++ {
		path[i].node = path[i].node.copy()
	}
	for i := len(path) - 2; i >= 0; i-- {
		entry := path[i]
		if !path[i+1].node.isEmpty() {
			storedLink, err := m.store(path[i+1].node)
			if err != nil {
				return fmt.Errorf("store new node: %w", err)
			}
			entry.node.Link[entry.linkIndex] = storedLink
		} else {
			entry.node.Link[entry.linkIndex] = nil
		}
	}
	if !path[0].node.isEmpty() {
		newRoot, err := m.store(path[0].node)
		if err != nil {
			return fmt.Errorf("store new root: %w", err)
		}
		m.root = newRoot
	} else {
		m.root = nil
	}
	return nil
}

// Splits the given node into two: left and right, so they could be the left+right children of a parent entry with the given key. The key is not expected to already be present in the source node, and will panic--but it would not migrated to the output, so that the caller can decide where to put it and its new children.
func split(node *mastNode, key interface{}, mast *Mast) (interface{}, interface{}, error) {
	var splitIndex int
	var err error
	for splitIndex = 0; splitIndex < len(node.Key); splitIndex++ {
		cmp, err := mast.keyOrder(node.Key[splitIndex], key)
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
	var leftLink, tooBigLink interface{} = nil, nil
	left := mastNode{
		Key:   append([]interface{}{}, node.Key[:splitIndex]...),
		Value: append([]interface{}{}, node.Value[:splitIndex]...),
		Link:  append([]interface{}{}, node.Link[:splitIndex+1]...),
	}

	// repartition the left and right subtrees based on the new key
	leftMaxLink := left.Link[len(left.Link)-1]
	if leftMaxLink != nil {
		leftMax, err := mast.load(leftMaxLink)
		if mast.debug {
			fmt.Printf("  splitting leftMax, node with keys: %v\n", leftMax.Key)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("loading leftMax: %w", err)
		}
		leftMaxLink, tooBigLink, err = split(leftMax, key, mast)
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
	var rightLink interface{}
	var right mastNode
	right = mastNode{
		Key:   append([]interface{}{}, node.Key[splitIndex:]...),
		Value: append([]interface{}{}, node.Value[splitIndex:]...),
		Link:  append([]interface{}{tooBigLink}, node.Link[splitIndex+1:]...),
	}
	rightMinLink := right.Link[0]
	if rightMinLink != nil {
		rightMin, err := mast.load(rightMinLink)
		if err != nil {
			return nil, nil, fmt.Errorf("load rightMin: %w", err)
		}
		tooSmallLink, rightMinLink, err := split(rightMin, key, mast)
		if err != nil {
			return nil, nil, fmt.Errorf("split rightMin: %w", err)
		}
		if mast.debug {
			fmt.Printf("  splitting rightMin, node with keys %v, is done: tooSmallLink=%v, rightMinLink=%v", rightMin.Key, tooSmallLink, rightMinLink)
		}
		right.Link[0] = rightMinLink
		if tooSmallLink != nil {
			// shouldn't happen
			panic("dunno what to do with non-nil tooSmall")
		}
	}
	if !right.isEmpty() {
		rightLink, err = mast.store(&right)
		if err != nil {
			return nil, nil, err
		}
	}
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

func (node *mastNode) findNode(m *Mast, key interface{}, options *findOptions) (*mastNode, int, error) {
	i := len(node.Key)
	if len(node.Link) != i+1 {
		node.dump("  ", m)
		panic(fmt.Sprintf("node %p doesn't have N+1 links", node))
	}
	for i := range node.Link {
		var err error
		var cmp int
		if i >= len(node.Key) {
			// going right
			cmp = 1
		} else {
			cmp, err = m.keyOrder(node.Key[i], key)
			if err != nil {
				return nil, 0, fmt.Errorf("keyCompare: %w", err)
			}
		}
		if cmp < 0 {
			continue
		}
		if cmp == 0 {
			options.path = append(options.path,
				pathEntry{node, i})
			return node, i, nil
		}
		if options.currentHeight == options.targetLayer {
			options.path = append(options.path,
				pathEntry{node, i})
			return node, i, nil
		}

		child, err := node.follow(i, options.createMissingNodes, m)
		if err != nil {
			return nil, 0, fmt.Errorf("following %d: %w", i, err)
		}
		options.currentHeight--
		options.path = append(options.path,
			pathEntry{node, i})
		return child.findNode(m, key, options)
	}
	options.path = append(options.path,
		pathEntry{node, i})
	return node, i, nil
}

func (node *mastNode) follow(i int, createOk bool, mast *Mast) (*mastNode, error) {
	if node.Link[i] != nil {
		child, err := mast.load(node.Link[i])
		if err != nil {
			return nil, fmt.Errorf("follow load %v: %w", node.Link[i], err)
		}
		return child, nil
	} else if !createOk {
		return node, nil
	} else {
		return emptyNodePointer(), nil
	}
}

func uint8min(x uint8, y uint8) uint8 {
	if x < y {
		return x
	}
	return y
}

func emptyNode() mastNode {
	return mastNode{
		Key:   []interface{}{},
		Value: []interface{}{},
		Link:  []interface{}{nil},
	}
}

func emptyNodePointer() *mastNode {
	node := emptyNode()
	return &node
}

func (node *mastNode) extract(from, to int) *mastNode {
	newChild := mastNode{}
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
	return &newChild
}

func (m *Mast) grow() error {
	var err error
	if m.debug {
		fmt.Printf("GROWING\n")
	}
	node, err := m.load(m.root)
	if err != nil {
		return fmt.Errorf("load root: %w", err)
	}
	newNode := emptyNode()
	start := 0
	for i, key := range node.Key {
		layer, err := m.keyLayer(key, m.branchFactor)
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
		validateNode(&newNode, m)
	}
	// if start <= len(node.Key) {
	newRightNode := node.extract(start, len(node.Key))
	if newRightNode != nil {
		if m.debug {
			fmt.Printf("extracted right:\n")
			newRightNode.dump("  ", m)
		}
		newRightLink, err := m.store(newRightNode)
		if err != nil {
			return err
		}
		newNode.Link[len(newNode.Link)-1] = newRightLink
	}
	validateNode(&newNode, m)
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

func (m *Mast) shrink() error {
	var err error
	if m.debug {
		fmt.Printf("SHRINKING\n")
		fmt.Printf("size=%d height=%d branchFactor=%d\n", m.size, m.height, m.branchFactor)
		m.dump()
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
	node, err := m.load(m.root)
	if err != nil {
		return fmt.Errorf("load root: %w", err)
	}
	newNode := mastNode{
		Key:   []interface{}{},
		Value: []interface{}{},
		Link:  []interface{}{},
	}
	for i := range node.Link {
		if node.Link[i] != nil {
			child, err := m.load(node.Link[i])
			if err != nil {
				return fmt.Errorf("load child: %w", err)
			}
			validateNode(child, m)
			newNode.Key = append(newNode.Key, child.Key[:]...)
			newNode.Value = append(newNode.Value, child.Value[:]...)
			newNode.Link = append(newNode.Link, child.Link[:]...)
			validateNode(&newNode, m)
		} else {
			newNode.Link = append(newNode.Link, nil)
		}
		if i < len(node.Key) {
			newNode.Key = append(newNode.Key, node.Key[i])
			newNode.Value = append(newNode.Value, node.Value[i])
		}
	}
	validateNode(&newNode, m)
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
		m.dump()
	}
	if m.shrinkBelowSize > 1 {
		m.shrinkBelowSize /= uint64(m.branchFactor)
		m.growAfterSize /= uint64(m.branchFactor)
	}
	return nil
}

func (m *Mast) contains(key interface{}) (bool, error) {
	if m.root == nil {
		return false, nil
	}
	node, err := m.load(m.root)
	if err != nil {
		return false, err
	}
	layer, err := m.keyLayer(key, m.branchFactor)
	if err != nil {
		return false, fmt.Errorf("layer: %w", err)
	}
	options := findOptions{
		targetLayer:   uint8min(layer, m.height),
		currentHeight: m.height,
	}
	node, i, err := node.findNode(m, key, &options)
	if err != nil {
		return false, err
	}
	if i >= len(node.Key) ||
		options.targetLayer != options.currentHeight {
		return false, nil
	}
	cmp, err := m.keyOrder(node.Key[i], key)
	if err != nil {
		return false, fmt.Errorf("keyCompare: %w", err)
	}
	return cmp == 0, nil
}

func (node *mastNode) dump(indent string, mast *Mast) error {
	str, err := node.string(indent, mast)
	if err != nil {
		return err
	}
	fmt.Printf("%s", str)
	return nil
}

func (node *mastNode) string(indent string, mast *Mast) (string, error) {
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
			res += fmt.Sprintf("}\n")
			continue
		}
		child, err := mast.load(node.Link[i])
		if err != nil {
			return "", err
		}
		res += "\n"
		childstr, err := child.string(indent+"   ", mast)
		if err != nil {
			return "", err
		}
		res += childstr
		res += indent + "}\n"
	}
	return res, nil
}

func (m *Mast) dump() error {
	if m.root == nil {
		fmt.Printf("NIL\n")
		return nil
	}
	node, err := m.load(m.root)
	if err != nil {
		return err
	}
	str, err := node.string("   ", m)
	if err != nil {
		return err
	}
	fmt.Printf("{\n%s}\n", str)
	return nil
}

func (node *mastNode) iter(f func(interface{}, interface{}) error, mast *Mast) error {
	if mast.debug {
		fmt.Printf("starting iter at node with keys: %v\n", node.Key)
	}
	for i, link := range node.Link {
		if link != nil {
			child, err := mast.load(link)
			if err != nil {
				return err
			}
			err = child.iter(f, mast)
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

func validateNode(node *mastNode, mast *Mast) {
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
		node.dump("  ", mast)
		fmt.Println("}")
		panic(fmt.Sprintf("node %p has %d links but %d keys", node, len(node.Link), len(node.Key)))
	}
	if len(node.Link) != len(node.Value)+1 {
		fmt.Println("DANGIT! {")
		node.dump("  ", mast)
		fmt.Println("}")
		panic(fmt.Sprintf("node %p has %d links but %d values", node, len(node.Link), len(node.Value)))
	}
}

func (m *Mast) mergeNodes(leftLink interface{}, rightLink interface{}) (interface{}, error) {
	if leftLink == nil {
		return rightLink, nil
	}
	if rightLink == nil {
		return leftLink, nil
	}
	left, err := m.load(leftLink)
	if err != nil {
		return nil, fmt.Errorf("load left: %w", err)
	}
	right, err := m.load(rightLink)
	if err != nil {
		return nil, fmt.Errorf("load right: %w", err)
	}
	combined := emptyNodePointer()
	combined.Key = make([]interface{}, len(left.Key)+len(right.Key))
	copy(combined.Key, left.Key)
	copy(combined.Key[len(left.Key):], right.Key)
	combined.Value = make([]interface{}, len(left.Value)+len(right.Value))
	copy(combined.Value, left.Value)
	copy(combined.Value[len(left.Value):], right.Value)
	combined.Link = make([]interface{}, len(left.Link)+len(right.Link)-1)
	copy(combined.Link, left.Link[0:len(left.Link)-1])
	copy(combined.Link[len(left.Link):], right.Link[1:])
	mergedLink, err := m.mergeNodes(left.Link[len(left.Link)-1], right.Link[0])
	if err != nil {
		return nil, fmt.Errorf("merge: %w", err)
	}
	combined.Link[len(left.Link)-1] = mergedLink
	combinedLink, err := m.store(combined)
	return combinedLink, nil
}

func (node *mastNode) copy() *mastNode {
	newNode := mastNode{
		make([]interface{}, len(node.Key)),
		make([]interface{}, len(node.Value)),
		make([]interface{}, len(node.Link)),
	}
	copy(newNode.Key, node.Key)
	copy(newNode.Value, node.Value)
	copy(newNode.Link, node.Link)
	return &newNode
}

func (m *Mast) checkRoot() error {
	node, err := m.load(m.root)
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
