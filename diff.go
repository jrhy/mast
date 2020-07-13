package mast

import "fmt"

type iterItem struct {
	considerLink interface{}
	yield        entry
}

func (newMast *Mast) diff(
	oldMast *Mast,
	entryCb func(added bool, removed bool, key interface{}, addedValue interface{}, removedValue interface{}) (bool, error),
	linkCb func(removed bool, link interface{}) (bool, error),
) error {
	alreadyNotifiedOldLink := map[uint8]interface{}{}
	alreadyNotifiedNewLink := map[uint8]interface{}{}
	oldStack := newIterItemStack(iterItem{considerLink: oldMast.root})
	newStack := newIterItemStack(iterItem{considerLink: newMast.root})
	for {
		if newMast.debug {
			fmt.Printf("diff() iteration:\n")
			fmt.Printf("  oldStack: %v\n", oldStack)
			fmt.Printf("  newStack: %v\n", newStack)
		}
		old := oldStack.pop()
		new := newStack.pop()
		if old == nil && new == nil {
			if newMast.debug {
				fmt.Printf("  done\n")
			}
			return nil
		} else if old == nil && new != nil {
			if new.considerLink != nil {
				if linkCb != nil && !newMast.alreadyNotified("new", alreadyNotifiedNewLink, new.considerLink) {
					keepGoing, err := linkCb(false, new.considerLink)
					if err != nil {
						return fmt.Errorf("callback: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
				newNode, err := newMast.load(new.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				newStack.pushNode(newNode, newMast)
			} else {
				if entryCb != nil {
					keepGoing, err := entryCb(true, false, new.yield.Key, new.yield.Value, nil)
					if err != nil {
						return fmt.Errorf("callback: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
			}
		} else if old != nil && new == nil {
			if old.considerLink != nil {
				if linkCb != nil && !oldMast.alreadyNotified("old", alreadyNotifiedOldLink, old.considerLink) {
					keepGoing, err := linkCb(true, old.considerLink)
					if err != nil {
						return fmt.Errorf("callback: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
				oldNode, err := oldMast.load(old.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				oldStack.pushNode(oldNode, oldMast)
			} else {
				if entryCb != nil {
					keepGoing, err := entryCb(false, true, old.yield.Key, nil, old.yield.Value)
					if err != nil {
						return fmt.Errorf("callback error: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
			}
		} else {
			if old.considerLink != nil && new.considerLink != nil {
				if old.considerLink != new.considerLink {
					if newMast.debug {
						fmt.Printf("  old(consider) new(consider) and links differ\n")
					}
					if linkCb != nil {
						if !oldMast.alreadyNotified("old", alreadyNotifiedOldLink, old.considerLink) {
							keepGoing, err := linkCb(true, old.considerLink)
							if err != nil {
								return fmt.Errorf("callback: %w", err)
							}
							if !keepGoing {
								return nil
							}
						}
						if !newMast.alreadyNotified("new", alreadyNotifiedNewLink, new.considerLink) {
							keepGoing, err := linkCb(false, new.considerLink)
							if err != nil {
								return fmt.Errorf("callback: %w", err)
							}
							if !keepGoing {
								return nil
							}
						}
					}
					oldNode, err := oldMast.load(old.considerLink)
					if err != nil {
						return fmt.Errorf("load: %w", err)
					}
					if len(oldNode.Link) == 1 {
						oldStack.pushLink(oldNode.Link[0])
						newStack.push(new)
						if newMast.debug {
							fmt.Printf("  oldStack descending through empty intermediate\n")
						}
						continue
					}
					oldKey := oldNode.Key[0]
					newNode, err := newMast.load(new.considerLink)
					if err != nil {
						return fmt.Errorf("load: %w", err)
					}
					if len(newNode.Link) == 1 {
						oldStack.push(old)
						newStack.pushLink(newNode.Link[0])
						if newMast.debug {
							fmt.Printf("  newStack descending through empty intermediate\n")
						}
						continue
					}
					newKey := newNode.Key[0]
					cmp, err := newMast.keyOrder(oldKey, newKey)
					if err != nil {
						return fmt.Errorf("keyCompare: %w", err)
					}
					if newMast.debug {
						fmt.Printf("  oldKey=%v.compare(newKey=%v): %d\n", oldKey, newKey, cmp)
					}
					if cmp < 0 {
						oldStack.pushNode(oldNode, oldMast)
						newStack.push(new)
					} else if cmp > 0 {
						oldStack.push(old)
						newStack.pushNode(newNode, newMast)
					} else {
						oldStack.pushNode(oldNode, oldMast)
						newStack.pushNode(newNode, newMast)
					}
				}
			} else if old.considerLink != nil && new.considerLink == nil {
				if linkCb != nil && !oldMast.alreadyNotified("old", alreadyNotifiedOldLink, old.considerLink) {
					keepGoing, err := linkCb(true, old.considerLink)
					if err != nil {
						return fmt.Errorf("callback: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
				oldNode, err := oldMast.load(old.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				oldStack.pushNode(oldNode, oldMast)
				newStack.push(new)
			} else if old.considerLink == nil && new.considerLink != nil {
				if linkCb != nil && !newMast.alreadyNotified("new", alreadyNotifiedNewLink, new.considerLink) {
					keepGoing, err := linkCb(false, new.considerLink)
					if err != nil {
						return fmt.Errorf("callback: %w", err)
					}
					if !keepGoing {
						return nil
					}
				}
				newNode, err := newMast.load(new.considerLink)
				if err != nil {
					return fmt.Errorf("load: %w", err)
				}
				oldStack.push(old)
				newStack.pushNode(newNode, newMast)
			} else {
				// both yields
				cmp, err := newMast.keyOrder(old.yield.Key, new.yield.Key)
				if err != nil {
					return fmt.Errorf("keyCompare: %w", err)
				}
				if cmp < 0 {
					newStack.push(new)
					if entryCb != nil {
						keepGoing, err := entryCb(false, true, old.yield.Key, nil, old.yield.Value)
						if err != nil {
							return fmt.Errorf("callback error: %w", err)
						}
						if !keepGoing {
							return nil
						}
					}
				} else if cmp == 0 {
					if old.yield.Value != new.yield.Value {
						if entryCb != nil {
							keepGoing, err := entryCb(true, true, new.yield.Key, new.yield.Value, old.yield.Value)
							if err != nil {
								return fmt.Errorf("callback error: %w", err)
							}
							if !keepGoing {
								return nil
							}
						}
					}
				} else {
					oldStack.push(old)
					if entryCb != nil {
						keepGoing, err := entryCb(true, false, new.yield.Key, new.yield.Value, nil)
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

func (m *Mast) alreadyNotified(name string, linkByHeight map[uint8]interface{}, link interface{}) bool {
	path := []interface{}{}
	myLink := link
	var keyHeight uint8
	for {
		path = append(path, myLink)
		node, err := m.load(myLink)
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

func (stack *iterItemStack) pushNode(node *mastNode, mast *Mast) {
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
