package mast

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

type iterItem struct {
	considerLink interface{}
	yield        entry
}

type diffState struct {
	alreadyNotifiedOldLink map[uint8]interface{}
	alreadyNotifiedNewLink map[uint8]interface{}
	oldStack               iterItemStack
	newStack               iterItemStack
	oldMast                *Mast
	addedLink              interface{}
	removedLink            interface{}
	curKey                 interface{}
	hasAdd                 bool
	hasRemove              bool
	addedValue             interface{}
	removedValue           interface{}
}

func newDiffState(oldMast *Mast, newMast *Mast) *diffState {
	var dc diffState
	dc.alreadyNotifiedOldLink = map[uint8]interface{}{}
	dc.alreadyNotifiedNewLink = map[uint8]interface{}{}
	if oldMast != nil {
		dc.oldMast = oldMast
		dc.oldStack = newIterItemStack(iterItem{considerLink: oldMast.root})
	}
	dc.newStack = newIterItemStack(iterItem{considerLink: newMast.root})
	return &dc
}

func (dc *diffState) resetCurrent() {
	dc.addedLink = nil
	dc.removedLink = nil
	dc.curKey = nil
	dc.addedValue = nil
	dc.removedValue = nil
	dc.hasAdd = false
	dc.hasRemove = false
}

var ErrNoMoreDiffs = errors.New("no more differences")

func (m *Mast) diff(
	ctx context.Context,
	oldMast *Mast,
	entryCb func(added bool, removed bool, key interface{}, addedValue interface{}, removedValue interface{}) (bool, error),
	linkCb func(removed bool, link interface{}) (bool, error),
) error {
	dc := newDiffState(oldMast, m)
	for {
		dc.resetCurrent()
		err := m.diffOne(ctx, dc)
		if err == ErrNoMoreDiffs {
			return nil
		}
		if err != nil {
			return err
		}
		if dc.addedLink != nil || dc.removedLink != nil {
			if linkCb == nil {
				continue
			}
			if dc.removedLink != nil {
				keepGoing, err := linkCb(true, dc.removedLink)
				if err != nil {
					return fmt.Errorf("callback: %w", err)
				}
				if !keepGoing {
					return nil
				}
			}
			if dc.addedLink != nil {
				keepGoing, err := linkCb(false, dc.addedLink)
				if err != nil {
					return fmt.Errorf("callback: %w", err)
				}
				if !keepGoing {
					return nil
				}
			}
		} else if dc.curKey != nil {
			if entryCb == nil {
				continue
			}
			keepGoing, err := entryCb(dc.hasAdd, dc.hasRemove, dc.curKey, dc.addedValue, dc.removedValue)
			if err != nil {
				return fmt.Errorf("callback: %w", err)
			}
			if !keepGoing {
				return nil
			}
		}
	}
}

func (m *Mast) diffOne(
	ctx context.Context,
	dc *diffState,
) error {
	if m.debug {
		fmt.Printf("diff() iteration:\n")
		fmt.Printf("  oldStack: %v\n", dc.oldStack)
		fmt.Printf("  newStack: %v\n", dc.newStack)
	}
	o := dc.oldStack.pop()
	n := dc.newStack.pop()
	if o == nil && n == nil {
		if m.debug {
			fmt.Printf("  done\n")
		}
		return ErrNoMoreDiffs
	} else if o == nil && n != nil {
		if n.considerLink != nil {
			if !m.alreadyNotified(ctx, "new", dc.alreadyNotifiedNewLink, n.considerLink) {
				dc.addedLink = n.considerLink
			}
			newNode, err := m.load(ctx, n.considerLink)
			if err != nil {
				return fmt.Errorf("load: %w", err)
			}
			dc.newStack.pushNode(newNode)
		} else {
			dc.curKey = n.yield.Key
			dc.addedValue = n.yield.Value
			dc.hasAdd = true
		}
	} else if o != nil && n == nil {
		if o.considerLink != nil {
			if !dc.oldMast.alreadyNotified(ctx, "old", dc.alreadyNotifiedOldLink, o.considerLink) {
				dc.removedLink = o.considerLink
			}
			oldNode, err := dc.oldMast.load(ctx, o.considerLink)
			if err != nil {
				return fmt.Errorf("load: %w", err)
			}
			dc.oldStack.pushNode(oldNode)
		} else {
			dc.curKey = o.yield.Key
			dc.removedValue = o.yield.Value
			dc.hasRemove = true
		}
	} else {
		if o.considerLink != nil && n.considerLink != nil {
			if o.considerLink != n.considerLink {
				if m.debug {
					fmt.Printf("  old(consider) new(consider) and links differ\n")
				}
				if !dc.oldMast.alreadyNotified(ctx, "old", dc.alreadyNotifiedOldLink, o.considerLink) {
					dc.removedLink = o.considerLink
				}
				if !m.alreadyNotified(ctx, "new", dc.alreadyNotifiedNewLink, n.considerLink) {
					dc.addedLink = n.considerLink
				}
				oldNode, err := dc.oldMast.load(ctx, o.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				if len(oldNode.Link) == 1 {
					dc.oldStack.pushLink(oldNode.Link[0])
					dc.newStack.push(n)
					if m.debug {
						fmt.Printf("  oldStack descending through empty intermediate\n")
					}
					return nil
				}
				oldKey := oldNode.Key[0]
				newNode, err := m.load(ctx, n.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				if len(newNode.Link) == 1 {
					dc.oldStack.push(o)
					dc.newStack.pushLink(newNode.Link[0])
					if m.debug {
						fmt.Printf("  newStack descending through empty intermediate\n")
					}
					return nil
				}
				newKey := newNode.Key[0]
				cmp, err := m.keyOrder(oldKey, newKey)
				if err != nil {
					return fmt.Errorf("keyCompare: %w", err)
				}
				if m.debug {
					fmt.Printf("  oldKey=%v.compare(newKey=%v): %d\n", oldKey, newKey, cmp)
				}
				if cmp < 0 {
					dc.oldStack.pushNode(oldNode)
					dc.newStack.push(n)
				} else if cmp > 0 {
					dc.oldStack.push(o)
					dc.newStack.pushNode(newNode)
				} else {
					dc.oldStack.pushNode(oldNode)
					dc.newStack.pushNode(newNode)
				}
			}
		} else if o.considerLink != nil && n.considerLink == nil {
			if !dc.oldMast.alreadyNotified(ctx, "old", dc.alreadyNotifiedOldLink, o.considerLink) {
				dc.removedLink = o.considerLink
			}
			oldNode, err := dc.oldMast.load(ctx, o.considerLink)
			if err != nil {
				return fmt.Errorf("load: %w", err)
			}
			dc.oldStack.pushNode(oldNode)
			dc.newStack.push(n)
		} else if o.considerLink == nil && n.considerLink != nil {
			if !m.alreadyNotified(ctx, "new", dc.alreadyNotifiedNewLink, n.considerLink) {
				dc.addedLink = n.considerLink
			}
			newNode, err := m.load(ctx, n.considerLink)
			if err != nil {
				return fmt.Errorf("load: %w", err)
			}
			dc.oldStack.push(o)
			dc.newStack.pushNode(newNode)
		} else {
			// both yields
			cmp, err := m.keyOrder(o.yield.Key, n.yield.Key)
			if err != nil {
				return fmt.Errorf("keyCompare: %w", err)
			}
			if cmp < 0 {
				dc.newStack.push(n)
				dc.curKey = o.yield.Key
				dc.removedValue = o.yield.Value
				dc.hasRemove = true
			} else if cmp == 0 {
				if !reflect.DeepEqual(o.yield.Value, n.yield.Value) {
					dc.curKey = o.yield.Key
					dc.addedValue = n.yield.Value
					dc.removedValue = o.yield.Value
				}
			} else {
				dc.oldStack.push(o)
				dc.curKey = n.yield.Key
				dc.addedValue = n.yield.Value
				dc.hasAdd = true
			}
		}
	}
	return nil
}

func (m *Mast) alreadyNotified(ctx context.Context, name string, linkByHeight map[uint8]interface{}, link interface{}) bool {
	path := []interface{}{}
	myLink := link
	var keyHeight uint8
	for {
		path = append(path, myLink)
		node, err := m.load(ctx, myLink)
		if err != nil {
			return false
		}
		if len(node.Link) == 1 {
			myLink = node.Link[0]
			continue
		}
		key := node.Key[0]
		keyHeight, err = m.keyLayer(key, m.branchFactor)
		if err != nil {
			return false
		}
		break
	}
	res := false
	for i, l := range path {
		if l != link {
			continue
		}
		if linkByHeight[keyHeight+uint8(i)] == link {
			res = true
		} else {
			linkByHeight[keyHeight+uint8(i)] = l
		}
	}
	if res && m.debug {
		fmt.Printf("already notified %s\n", name)
	}
	return res
}

type iterItemStack struct {
	things []iterItem
}

func newIterItemStack(item iterItem) iterItemStack {
	return iterItemStack{
		[]iterItem{item},
	}
}

func (stack *iterItemStack) pop() *iterItem {
	if len(stack.things) > 0 {
		popped := stack.things[len(stack.things)-1]
		stack.things = stack.things[0 : len(stack.things)-1]
		return &popped
	}
	return nil
}

func (stack *iterItemStack) pushNode(node *mastNode) {
	for n := range node.Key {
		i := len(node.Key) - n
		stack.pushLink(node.Link[i])
		stack.pushYield(node, i-1)
	}
	stack.pushLink(node.Link[0])
}

func (stack *iterItemStack) pushLink(link interface{}) {
	if link != nil {
		stack.push(&iterItem{considerLink: link})
	}
}

func (stack *iterItemStack) pushYield(node *mastNode, i int) {
	stack.push(&iterItem{yield: entry{node.Key[i], node.Value[i]}})
}

func (stack *iterItemStack) push(item *iterItem) {
	stack.things = append(stack.things, *item)
}
