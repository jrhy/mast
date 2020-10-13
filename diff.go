package mast

import (
	"context"
	"fmt"
)

type iterItem struct {
	considerLink interface{}
	yield        entry
}

func (m *Mast) diff(
	ctx context.Context,
	oldMast *Mast,
	entryCb func(added bool, removed bool, key interface{}, addedValue interface{}, removedValue interface{}) (bool, error),
	linkCb func(removed bool, link interface{}) (bool, error),
) error {
	alreadyNotifiedOldLink := map[uint8]interface{}{}
	alreadyNotifiedNewLink := map[uint8]interface{}{}
	var oldStack iterItemStack
	if oldMast != nil {
		oldStack = newIterItemStack(iterItem{considerLink: oldMast.root})
	}
	newStack := newIterItemStack(iterItem{considerLink: m.root})
	for {
		if m.debug {
			fmt.Printf("diff() iteration:\n")
			fmt.Printf("  oldStack: %v\n", oldStack)
			fmt.Printf("  newStack: %v\n", newStack)
		}
		o := oldStack.pop()
		n := newStack.pop()
		if o == nil && n == nil {
			if m.debug {
				fmt.Printf("  done\n")
			}
			return nil
		} else if o == nil && n != nil {
			if n.considerLink != nil {
				if linkCb != nil && !m.alreadyNotified(ctx, "new", alreadyNotifiedNewLink, n.considerLink) {
					keepGoing, err := linkCb(false, n.considerLink)
					if err != nil {
						return fmt.Errorf("callback: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
				newNode, err := m.load(ctx, n.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				newStack.pushNode(newNode)
			} else if entryCb != nil {
				keepGoing, err := entryCb(true, false, n.yield.Key, n.yield.Value, nil)
				if err != nil {
					return fmt.Errorf("callback: %w", err)
				}
				if !keepGoing {
					return nil
				}
			}
		} else if o != nil && n == nil {
			if o.considerLink != nil {
				if linkCb != nil && !oldMast.alreadyNotified(ctx, "old", alreadyNotifiedOldLink, o.considerLink) {
					keepGoing, err := linkCb(true, o.considerLink)
					if err != nil {
						return fmt.Errorf("callback: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
				oldNode, err := oldMast.load(ctx, o.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				oldStack.pushNode(oldNode)
			} else if entryCb != nil {
				keepGoing, err := entryCb(false, true, o.yield.Key, nil, o.yield.Value)
				if err != nil {
					return fmt.Errorf("callback error: %w", err)
				}
				if !keepGoing {
					return nil
				}
			}
		} else {
			if o.considerLink != nil && n.considerLink != nil {
				if o.considerLink != n.considerLink {
					if m.debug {
						fmt.Printf("  old(consider) new(consider) and links differ\n")
					}
					if linkCb != nil {
						if !oldMast.alreadyNotified(ctx, "old", alreadyNotifiedOldLink, o.considerLink) {
							keepGoing, err := linkCb(true, o.considerLink)
							if err != nil {
								return fmt.Errorf("callback: %w", err)
							}
							if !keepGoing {
								return nil
							}
						}
						if !m.alreadyNotified(ctx, "new", alreadyNotifiedNewLink, n.considerLink) {
							keepGoing, err := linkCb(false, n.considerLink)
							if err != nil {
								return fmt.Errorf("callback: %w", err)
							}
							if !keepGoing {
								return nil
							}
						}
					}
					oldNode, err := oldMast.load(ctx, o.considerLink)
					if err != nil {
						return fmt.Errorf("load: %w", err)
					}
					if len(oldNode.Link) == 1 {
						oldStack.pushLink(oldNode.Link[0])
						newStack.push(n)
						if m.debug {
							fmt.Printf("  oldStack descending through empty intermediate\n")
						}
						continue
					}
					oldKey := oldNode.Key[0]
					newNode, err := m.load(ctx, n.considerLink)
					if err != nil {
						return fmt.Errorf("load: %w", err)
					}
					if len(newNode.Link) == 1 {
						oldStack.push(o)
						newStack.pushLink(newNode.Link[0])
						if m.debug {
							fmt.Printf("  newStack descending through empty intermediate\n")
						}
						continue
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
						oldStack.pushNode(oldNode)
						newStack.push(n)
					} else if cmp > 0 {
						oldStack.push(o)
						newStack.pushNode(newNode)
					} else {
						oldStack.pushNode(oldNode)
						newStack.pushNode(newNode)
					}
				}
			} else if o.considerLink != nil && n.considerLink == nil {
				if linkCb != nil && !oldMast.alreadyNotified(ctx, "old", alreadyNotifiedOldLink, o.considerLink) {
					keepGoing, err := linkCb(true, o.considerLink)
					if err != nil {
						return fmt.Errorf("callback: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
				oldNode, err := oldMast.load(ctx, o.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				oldStack.pushNode(oldNode)
				newStack.push(n)
			} else if o.considerLink == nil && n.considerLink != nil {
				if linkCb != nil && !m.alreadyNotified(ctx, "new", alreadyNotifiedNewLink, n.considerLink) {
					keepGoing, err := linkCb(false, n.considerLink)
					if err != nil {
						return fmt.Errorf("callback: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
				newNode, err := m.load(ctx, n.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				oldStack.push(o)
				newStack.pushNode(newNode)
			} else {
				// both yields
				cmp, err := m.keyOrder(o.yield.Key, n.yield.Key)
				if err != nil {
					return fmt.Errorf("keyCompare: %w", err)
				}
				if cmp < 0 {
					newStack.push(n)
					if entryCb != nil {
						keepGoing, err := entryCb(false, true, o.yield.Key, nil, o.yield.Value)
						if err != nil {
							return fmt.Errorf("callback error: %w", err)
						}
						if !keepGoing {
							return nil
						}
					}
				} else if cmp == 0 {
					if o.yield.Value != n.yield.Value {
						if entryCb != nil {
							keepGoing, err := entryCb(false, false, n.yield.Key, n.yield.Value, o.yield.Value)
							if err != nil {
								return fmt.Errorf("callback error: %w", err)
							}
							if !keepGoing {
								return nil
							}
						}
					}
				} else {
					oldStack.push(o)
					if entryCb != nil {
						keepGoing, err := entryCb(true, false, n.yield.Key, n.yield.Value, nil)
						if err != nil {
							return fmt.Errorf("callback error: %w", err)
						}
						if !keepGoing {
							return nil
						}
					}
				}
			}
		}
	}
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
