package mast

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
	"github.com/leanovate/gopter/gen"
	"github.com/stretchr/testify/assert"
)

var testThingy *testing.T

type expected struct {
	entries  map[uint]uint
	snapshot []map[uint]uint
}

type system struct {
	m         *Mast
	snapshot  []*Mast
	cmdCount  int
	nodeCache NodeCache
}

type xentry struct {
	Key   uint
	Value uint
}

const (
	uimax      = 99_999
	nSnapshots = 5
)

var (
	cmdCount = 0
	debug    = false
)

func progress(i interface{}) {
	if debug {
		fmt.Printf("%v\n", i)
	}
}

var FlushCommand = &commands.ProtoCommand{
	Name: "Flush",
	RunFunc: func(s commands.SystemUnderTest) commands.Result {
		_, err := s.(*system).m.flush()
		if err != nil {
			return err
		}
		s.(*system).cmdCount++
		return nil
	},
	NextStateFunc:    func(state commands.State) commands.State { return state },
	PreConditionFunc: func(state commands.State) bool { return true },
	PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
		if result != nil {
			fmt.Printf("flush PostCondition: %v\n", result)
			return &gopter.PropResult{Status: gopter.PropFalse}
		}
		progress("Flush")
		return &gopter.PropResult{Status: gopter.PropTrue}
	},
}

var SizeCommand = &commands.ProtoCommand{
	Name: "Size",
	RunFunc: func(s commands.SystemUnderTest) commands.Result {
		s.(*system).cmdCount++
		return s.(*system).m.Size()
	},
	NextStateFunc:    func(state commands.State) commands.State { return state },
	PreConditionFunc: func(state commands.State) bool { return true },
	PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
		if uint64(len(state.(*expected).entries)) != result.(uint64) {
			fmt.Printf("sizeCommandPostCondition: expected=%d, actual=%d\n", uint64(len(state.(*expected).entries)), result.(uint64))
			return &gopter.PropResult{Status: gopter.PropFalse}
		}
		progress("Size")
		return &gopter.PropResult{Status: gopter.PropTrue}
	},
}

type diffLinksCommand uint

func (n diffLinksCommand) Run(s commands.SystemUnderTest) commands.Result {
	slot := int(n) % nSnapshots
	new := s.(*system).m
	old := s.(*system).snapshot[slot]
	_, err := old.flush()
	if err != nil {
		return fmt.Errorf("flush old: %w", err)
	}
	_, err = new.flush()
	if err != nil {
		return fmt.Errorf("flush new: %w", err)
	}
	empty := newTestTree(uint(0), uint(0))
	empty.branchFactor = 3
	empty.growAfterSize = 3
	/*
		showLinkDiff("empty->old link diff", &empty, old)
		new.debug = true
		showLinkDiff("old->new link diff", old, new)
		showLinkDiff("empty->new link diff", &empty, new)
		new.debug = false
		fmt.Printf("old dump from root %v:\n", old.root)
		old.dump()
		fmt.Printf("new dump from root %v:\n", new.root)
		new.dump()
	*/
	newLinks := map[string]struct{}{}
	newLinks1 := map[string]struct{}{}
	newLinks2 := map[string]struct{}{}
	// show why the union of these two diffs isn't the same as new.DiffLinks(&empty)
	err = old.DiffLinks(&empty,
		func(removed bool, link interface{}) (bool, error) {
			if !removed {
				ls := link.(string)
				if _, alreadyIn := newLinks1[ls]; alreadyIn {
					return false, fmt.Errorf("link %v was already noted", ls)
				}
				newLinks1[ls] = struct{}{}
				newLinks[ls] = struct{}{}
			}
			return true, nil
		})
	if err != nil {
		return fmt.Errorf("diffLinks old: %w", err)
	}
	err = new.DiffLinks(old,
		func(removed bool, link interface{}) (bool, error) {
			if !removed {
				ls := link.(string)
				if _, alreadyIn := newLinks2[ls]; alreadyIn {
					return false, fmt.Errorf("link %v was already noted", ls)
				}
				newLinks[ls] = struct{}{}
				newLinks2[ls] = struct{}{}
			}
			return true, nil
		})
	if err != nil {
		return fmt.Errorf("diffLinks new: %w", err)
	}

	// fmt.Printf("newLinks: %v\n", newLinks)
	syncedNew := *new
	syncedNew.persist = NewInMemoryStore()
	for link := range newLinks {
		bytes, err := new.persist.Load(link)
		if err != nil {
			return fmt.Errorf("load newLink: %w", err)
		}
		err = syncedNew.persist.Store(link, bytes)
		if err != nil {
			return fmt.Errorf("store newLink: %w", err)
		}
	}

	diffs := map[bool]map[uint]uint{
		false: {},
		true:  {},
	}
	err = syncedNew.DiffIter(old,
		func(added bool, removed bool, k interface{}, addedValue interface{}, removedValue interface{}) (bool, error) {
			if added {
				diffs[false][uint(k.(uint))] = addedValue.(uint)
			}
			if removed {
				diffs[removed][uint(k.(uint))] = removedValue.(uint)
			}
			return true, nil
		})
	if err != nil {
		return fmt.Errorf("diffIter: %w", err)
	}
	return diffs
}

func showLinkDiff(desc string, old *Mast, new *Mast) {
	fmt.Printf("%s:\n", desc)
	new.DiffLinks(old,
		func(removed bool, link interface{}) (bool, error) {
			verb := "added"
			if removed {
				verb = "removed"
			}
			fmt.Printf("%s %v\n", verb, link)
			return true, nil
		})
}

func (n diffLinksCommand) NextState(state commands.State) commands.State {
	return state
}

func (n diffLinksCommand) PreCondition(state commands.State) bool {
	return state.(*expected).snapshot[int(n)%nSnapshots] != nil
}

func (n diffLinksCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diffs := map[bool]map[uint]uint{
		false: {},
		true:  {},
	}
	slot := int(n) % nSnapshots
	new := state.(*expected).entries
	old := state.(*expected).snapshot[slot]
	for k, v := range new {
		oldVal, oldHasKey := old[k]
		if oldHasKey && oldVal != v {
			diffs[true][k] = oldVal
			diffs[false][k] = v
		} else if !oldHasKey {
			diffs[false][k] = v
		}
	}
	for k, v := range old {
		_, newHasKey := new[k]
		if !newHasKey {
			diffs[true][k] = v
		}
	}
	switch result := result.(type) {
	case error:
		fmt.Printf("diffLinks: %v\n", result)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	actual := result.(map[bool]map[uint]uint)
	if !reflect.DeepEqual(diffs, actual) {
		assert.Equal(testThingy, diffs, actual)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	// fmt.Printf("expected %v\n", diffs)
	// fmt.Printf("actual    %v\n", actual)
	// if len(actual[true]) > 0 {
	// fmt.Printf("YAY\n")
	// }
	progress(n)
	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (n diffLinksCommand) String() string {
	slot := int(n) % nSnapshots
	return fmt.Sprintf("DiffLinks(%d)", slot)
}

var genDiffLinks = uintCommandGen(
	func(slot uint) commands.Command { return diffLinksCommand(slot) },
	func(command interface{}) uint { return uint(command.(diffLinksCommand)) })

type diffCommand uint

func (n diffCommand) Run(s commands.SystemUnderTest) commands.Result {
	slot := int(n) % nSnapshots
	old := s.(*system).snapshot[slot]
	diffs := map[bool]map[uint]uint{
		false: {},
		true:  {},
	}
	err := s.(*system).m.DiffIter(old,
		func(added bool, removed bool, k interface{}, addedValue interface{}, removedValue interface{}) (bool, error) {
			if added {
				diffs[false][uint(k.(uint))] = addedValue.(uint)
			}
			if removed {
				diffs[true][uint(k.(uint))] = removedValue.(uint)
			}
			return true, nil
		})
	if err != nil {
		return fmt.Errorf("diffIter: %w", err)
	}
	s.(*system).cmdCount++
	return diffs
}

func (n diffCommand) NextState(state commands.State) commands.State {
	return state
}

func (n diffCommand) PreCondition(state commands.State) bool {
	return state.(*expected).snapshot[int(n)%nSnapshots] != nil
}

func (n diffCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	diffs := map[bool]map[uint]uint{
		false: {},
		true:  {},
	}
	slot := int(n) % nSnapshots
	new := state.(*expected).entries
	old := state.(*expected).snapshot[slot]
	for k, v := range new {
		oldVal, oldHasKey := old[k]
		if oldHasKey && oldVal != v {
			diffs[true][k] = oldVal
			diffs[false][k] = v
		} else if !oldHasKey {
			diffs[false][k] = v
		}
	}
	for k, v := range old {
		_, newHasKey := new[k]
		if !newHasKey {
			diffs[true][k] = v
		}
	}
	switch result := result.(type) {
	case error:
		fmt.Printf("diff: %v\n", result)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	actual := result.(map[bool]map[uint]uint)
	if !reflect.DeepEqual(diffs, actual) {
		assert.Equal(testThingy, diffs, actual)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	// fmt.Printf("expected %v\n", diffs)
	// fmt.Printf("actual    %v\n", actual)
	// if len(actual[true]) > 0 {
	// fmt.Printf("YAY\n")
	// }
	progress(n)
	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (n diffCommand) String() string {
	slot := int(n) % nSnapshots
	return fmt.Sprintf("Diff(%d)", slot)
}

var genDiff = uintCommandGen(
	func(slot uint) commands.Command { return diffCommand(slot) },
	func(command interface{}) uint { return uint(command.(diffCommand)) })

type snapshotCommand uint

func (n snapshotCommand) Run(s commands.SystemUnderTest) commands.Result {
	slot := int(n) % nSnapshots
	cur := *s.(*system).m
	snapshot, err := cur.Clone()
	if err != nil {
		return err
	}
	s.(*system).snapshot[slot] = &snapshot
	return nil
}

func (n snapshotCommand) NextState(state commands.State) commands.State {
	s := state.(*expected)
	slot := int(n) % nSnapshots
	snapshot := make(map[uint]uint, len(s.entries))
	for k, v := range s.entries {
		snapshot[k] = v
	}
	s.snapshot[slot] = snapshot
	return s
}

func (n snapshotCommand) PreCondition(state commands.State) bool {
	return true
}

func (n snapshotCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	switch result := result.(type) {
	case error:
		fmt.Printf("snapshotPostCondition: %v\n", result)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	progress(n)
	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (n snapshotCommand) String() string {
	slot := int(n) % nSnapshots
	return fmt.Sprintf("Snapshot(%d)", slot)
}

var genSnapshot = uintCommandGen(
	func(slot uint) commands.Command { return snapshotCommand(slot) },
	func(command interface{}) uint { return uint(command.(snapshotCommand)) })

type getCommand uint

func (value getCommand) Run(s commands.SystemUnderTest) commands.Result {
	// fmt.Printf("before get %v:\n", value)
	// s.(*system).m.dump()
	var val uint
	_, err := s.(*system).m.Get(uint(value), &val)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	s.(*system).cmdCount++
	return val
}

func (value getCommand) NextState(state commands.State) commands.State {
	return state
}

func (value getCommand) PreCondition(state commands.State) bool {
	return true
}

func (value getCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	expected, ok := state.(*expected).entries[uint(value)]
	if !ok && result == nil || expected == result {
		progress(value)
		return &gopter.PropResult{Status: gopter.PropTrue}
	}
	fmt.Printf("SURPRISE!!!\n")
	if !ok && result != nil {
		fmt.Printf("getCommandPostCondition: (value=%v) expected=!ok actual=%v\n", value, result)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	fmt.Printf("getCommandPostCondition: (value=%v) expected=%T %v actual=%T %v\n", value, expected, expected, result, result)
	return &gopter.PropResult{Status: gopter.PropFalse}
}

func (value getCommand) String() string {
	return fmt.Sprintf("Get(%d)", value)
}

var genGet = uintCommandGen(
	func(value uint) commands.Command { return getCommand(value) },
	func(command interface{}) uint { return uint(command.(getCommand)) })

type deleteCommand uint

func (value deleteCommand) Run(s commands.SystemUnderTest) commands.Result {
	err := s.(*system).m.Delete(uint(value), uint(value))
	if err != nil {
		fmt.Printf("was attempting to delete %d, %d in tree:\n", uint(value), uint(value))
		s.(*system).m.dump()
	}
	s.(*system).cmdCount++
	return err
}

func (value deleteCommand) NextState(state commands.State) commands.State {
	delete(state.(*expected).entries, uint(value))
	return state
}

func (value deleteCommand) PreCondition(state commands.State) bool {
	existingValue, present := state.(*expected).entries[uint(value)]
	return present && existingValue == uint(value)
}

func (value deleteCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	if result != nil {
		fmt.Printf("deletePostCondition: %v\n", result)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	progress(value)
	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (value deleteCommand) String() string {
	return fmt.Sprintf("Delete(%d,%d)", value, value)
}

var genDelete = uintCommandGen(
	func(value uint) commands.Command { return deleteCommand(value) },
	func(command interface{}) uint { return uint(command.(deleteCommand)) })

type deleteNthCommand uint

func (value deleteNthCommand) Run(s commands.SystemUnderTest) commands.Result {
	slice, err := s.(*system).m.toSlice()
	if err != nil {
		return fmt.Errorf("slice: %w", err)
	}
	entry := slice[uint(value)]
	err = s.(*system).m.Delete(entry.Key, entry.Value)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	s.(*system).cmdCount++
	return nil
}

func (value deleteNthCommand) NextState(state commands.State) commands.State {
	s := state.(*expected)
	var keys []int
	for k := range s.entries {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	nthKey := keys[uint(value)]
	delete(s.entries, uint(nthKey))
	return state
}

func (value deleteNthCommand) PreCondition(state commands.State) bool {
	s := state.(*expected)
	return int(value) < len(s.entries)
}

func (value deleteNthCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	if result != nil {
		fmt.Printf("deleteNthPostCondition: %v\n", result)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	progress(value)
	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (value deleteNthCommand) String() string {
	return fmt.Sprintf("DeleteNth(%d)", value)
}

var genDeleteNth = uintCommandGen(
	func(value uint) commands.Command { return deleteNthCommand(value) },
	func(command interface{}) uint { return uint(command.(deleteNthCommand)) })

type insertCommand uint

func (value insertCommand) Run(s commands.SystemUnderTest) commands.Result {
	err := s.(*system).m.Insert(uint(value), uint(value))
	if err != nil {
		return err
	}
	s.(*system).cmdCount++
	return nil
}

func (value insertCommand) NextState(state commands.State) commands.State {
	s := state.(*expected)
	s.entries[uint(value)] = uint(value)
	return state
}

func (value insertCommand) PreCondition(state commands.State) bool {
	s := state.(*expected)
	existing, present := s.entries[uint(value)]
	return !present || existing == uint(value)
}

func (value insertCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	if result != nil {
		fmt.Printf("insertCommandPostCondition: %v\n", result)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	progress(value)
	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (value insertCommand) String() string {
	return fmt.Sprintf("Insert(%d,%d)", value, value)
}

var genInsert = uintCommandGen(
	func(value uint) commands.Command { return insertCommand(value) },
	func(command interface{}) uint { return uint(command.(insertCommand)) })

type updateCommand uint

func (value updateCommand) Run(s commands.SystemUnderTest) commands.Result {
	err := s.(*system).m.Insert(uint(value), uint(value))
	if err != nil {
		return err
	}
	s.(*system).cmdCount++
	return nil
}

func (value updateCommand) NextState(state commands.State) commands.State {
	state.(*expected).entries[uint(value)] = uint(value)
	return state
}

func (value updateCommand) PreCondition(state commands.State) bool {
	s := state.(*expected)
	existing, present := s.entries[uint(value)]
	return present && existing != uint(value)
}

func (value updateCommand) PostCondition(state commands.State, result commands.Result) *gopter.PropResult {
	if result != nil {
		fmt.Printf("updateCommandPostCondition: %v\n", result)
		return &gopter.PropResult{Status: gopter.PropFalse}
	}
	progress(value)
	return &gopter.PropResult{Status: gopter.PropTrue}
}

func (value updateCommand) String() string {
	return fmt.Sprintf("Update(%d,%d)", value, value)
}

var genUpdate = uintCommandGen(
	func(value uint) commands.Command { return updateCommand(value) },
	func(command interface{}) uint { return uint(command.(updateCommand)) })

func entryCommandGen(toCommand func(xentry) commands.Command /*, fromCommand func(interface{}) entry*/) gopter.Gen {
	return gen.Struct(reflect.TypeOf(&xentry{}), map[string]gopter.Gen{
		"key":   gen.UIntRange(0, uimax),
		"value": gen.UIntRange(0, uimax),
	}).Map(func(entry xentry) commands.Command {
		return toCommand(entry)
	}) /*.WithShrinker(func(v interface{}) gopter.Shrink {
		return gen.UIntShrinker(fromCommand(v)).Map(func(entry entry) commands.Command {
			return toCommand(entry)
		})
	})*/
}

func uintCommandGen(toCommand func(uint) commands.Command, fromCommand func(interface{}) uint) gopter.Gen {
	return gen.UIntRange(0, uimax).Map(func(value uint) commands.Command {
		return toCommand(value)
	}).WithShrinker(func(v interface{}) gopter.Shrink {
		return gen.UIntShrinker(fromCommand(v)).Map(func(value uint) commands.Command {
			return toCommand(value)
		})
	})
}

var (
	maxHeight    uint8 = 0
	mastCommands       = &commands.ProtoCommands{
		NewSystemUnderTestFunc: func(initialState commands.State) commands.SystemUnderTest {
			m := newTestTree(uint(0), uint(0))
			m.branchFactor = 3
			m.growAfterSize = 3
			for key, value := range initialState.(*expected).entries {
				err := m.Insert(uint(key), uint(value))
				if err != nil {
					return err
				}
			}
			progress("NewSystem")
			return &system{&m, make([]*Mast, nSnapshots), 0, NewNodeCache(500)}
		},
		DestroySystemUnderTestFunc: func(s commands.SystemUnderTest) {
			mast := s.(*system).m
			if mast.height > maxHeight {
				maxHeight = mast.height
			}
			cmdCount += s.(*system).cmdCount
		},
		InitialStateGen: gen.MapOf(gen.UIntRange(0, uimax), gen.UIntRange(0, uimax)).Map(func(entries map[uint]uint) *expected {
			return &expected{
				entries:  entries,
				snapshot: make([]map[uint]uint, nSnapshots),
			}
		}),
		InitialPreConditionFunc: func(state commands.State) bool {
			_ = state.(*expected)
			return true
		},
		GenCommandFunc: func(state commands.State) gopter.Gen {
			return gen.Weighted(
				[]gen.WeightedGen{
					{Weight: 100, Gen: genDelete},
					{Weight: 100, Gen: genDeleteNth},
					{Weight: 1, Gen: genDiff},
					{Weight: 1, Gen: genDiffLinks},
					{Weight: 100, Gen: genGet},
					{Weight: 100, Gen: genInsert},
					{Weight: 5, Gen: genSnapshot},
					{Weight: 100, Gen: genUpdate},
					{Weight: 1, Gen: gen.Const(FlushCommand)},
					{Weight: 100, Gen: gen.Const(SizeCommand)},
				},
			)
		},
	}
)

func TestExerciser(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	// parameters := gopter.DefaultTestParametersWithSeed(1593228262585360000)
	if !testing.Short() {
		parameters.MaxSize = 2048
		// parameters.MinSuccessfulTests = 10_000
		// parameters.MinSize = 1024
	}
	properties := gopter.NewProperties(parameters)
	properties.Property("mast exerciser", commands.Prop(mastCommands))
	testThingy = t
	properties.TestingRun(t)
	testThingy = nil
	if !t.Failed() {
		assert.GreaterOrEqual(t, int(maxHeight), 4)
		fmt.Printf("biggest tree height: %d\n", maxHeight)
		fmt.Printf("successful commands: %d\n", cmdCount)
	}
}
