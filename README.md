![Go](https://github.com/jrhy/mast/workflows/Go/badge.svg)
[![GoDoc](https://godoc.org/github.com/jrhy/mast?status.svg)](https://godoc.org/github.com/jrhy/mast)

# mast
immutable, versioned, diffable map implementation of the Merkle Search Tree

`import "github.com/jrhy/mast"`

See Go package documentation at:

https://godoc.org/github.com/jrhy/mast

* [Overview](#pkg-overview)
* [Index](#pkg-index)
* [Examples](#pkg-examples)

## <a name="pkg-overview">Overview</a>
Package mast provides an immutable, versioned, diffable map
implementation of a Merkle Search Tree (MST).  Masts can be huge
(not limited to memory).  Masts can be stored in anything, like a
filesystem, KV store, or blob store.  Masts are designed to be
safely concurrently accessed by multiple threads/hosts.  And like
Merkle DAGs, Masts are designed to be easy to cache and synchronize.

Use case 1: Efficient storage of multiple versions of materialized views
Use case 2: Diffing of versions integrates CDC/streaming
Use case 3: Efficient copy-on-write alternative to Go builtin map

### What are MSTs
Mast is an implementation of the structure described in the
awesome paper, "Merkle
Search Trees: Efficient State-Based CRDTs in Open Networks", by
Alex Auvolat and François Taïani, 2019
(<a href="https://hal.inria.fr/hal-02303490/document">https://hal.inria.fr/hal-02303490/document</a>).

MSTs are similar to persistent B-Trees, except an element's layer
(distance to leaves) is deterministically calculated (e.g.  by a
hash function), obviating the need for complicated rebalancing or
rotations in the implementation, but more importantly resulting in
the amazing property of converging to the same shape even when
entries are inserted in different orders.  This makes MSTs an
interesting choice for conflict-free replicated data types (CRDTs).

MSTs are like other Merkle structures in that two instances can
easily be compared to confirm equality or find differences, since
equal node hashes indicate equal contents.

### Concurrency
A Mast can be Clone()d for sharing between threads. Cloning creates
a new version that can evolve independently, yet shares all the
unmodified subtrees with its parent, and as such are relatively
cheap to create.

### Inspiration
The immutable data types in Clojure, Haskell, ML and other functional
languages really do make it easier to "reason about" systems; easier
to test, provide a foundation to build more quickly on.

<a href="https://github.com/bodil/im-rs">https://github.com/bodil/im-rs</a>, "Blazing fast immutable collection
datatypes for Rust", by Bodil Stokke, is an exemplar: the diff algorithm
and use of property testing, are instructive, and Chunks and PoolRefs
fill gaps in understanding of Rust's ownership model for library writers
coming from GC'd languages.




## <a name="pkg-index">Index</a>
* [Constants](#pkg-constants)
* [type CreateRemoteOptions](#CreateRemoteOptions)
* [type Key](#Key)
* [type Mast](#Mast)
  * [func NewInMemory() Mast](#NewInMemory)
  * [func (m Mast) Clone() (Mast, error)](#Mast.Clone)
  * [func (m *Mast) Delete(key interface{}, value interface{}) error](#Mast.Delete)
  * [func (m *Mast) DiffIter(oldMast *Mast, f func(added, removed bool, key, addedValue, removedValue interface{}) (bool, error)) error](#Mast.DiffIter)
  * [func (m *Mast) DiffLinks(oldMast *Mast, f func(removed bool, link interface{}) (bool, error)) error](#Mast.DiffLinks)
  * [func (m *Mast) Get(k interface{}, value interface{}) (bool, error)](#Mast.Get)
  * [func (m *Mast) Insert(key interface{}, value interface{}) error](#Mast.Insert)
  * [func (m *Mast) Iter(f func(interface{}, interface{}) error) error](#Mast.Iter)
  * [func (m *Mast) MakeRoot() (*Root, error)](#Mast.MakeRoot)
  * [func (m Mast) Size() uint64](#Mast.Size)
* [type NodeCache](#NodeCache)
  * [func NewNodeCache(size int) NodeCache](#NewNodeCache)
* [type Persist](#Persist)
  * [func NewInMemoryStore() Persist](#NewInMemoryStore)
* [type RemoteConfig](#RemoteConfig)
* [type Root](#Root)
  * [func NewRoot(remoteOptions *CreateRemoteOptions) *Root](#NewRoot)
  * [func (r *Root) LoadMast(config RemoteConfig) (*Mast, error)](#Root.LoadMast)

#### <a name="pkg-examples">Examples</a>
* [Mast.DiffIter](#example_Mast_DiffIter)
* [Mast.Size](#example_Mast_Size)

#### <a name="pkg-files">Package files</a>
[diff.go](/src/mast/diff.go) [doc.go](/src/mast/doc.go) [in_memory_store.go](/src/mast/in_memory_store.go) [key.go](/src/mast/key.go) [lib.go](/src/mast/lib.go) [node_cache.go](/src/mast/node_cache.go) [pub.go](/src/mast/pub.go) [store.go](/src/mast/store.go) 


## <a name="pkg-constants">Constants</a>
``` go
const DefaultBranchFactor = 16
```
DefaultBranchFactor is how many entries per node a tree will normally have.





## <a name="CreateRemoteOptions">type</a> [CreateRemoteOptions](/src/mast/pub.go?s=162:299#L9)
``` go
type CreateRemoteOptions struct {
    // BranchFactor, or number of entries per node.  0 means use DefaultBranchFactor.
    BranchFactor uint
}

```
CreateRemoteOptions sets initial parameters for the tree, that would be painful to change after the tree has data.










## <a name="Key">type</a> [Key](/src/mast/key.go?s=121:405#L9)
``` go
type Key interface {
    // Layer can deterministically compute its ideal layer (distance from leaves) in a tree with the given branch factor.
    Layer(branchFactor uint) uint8
    // Order returns -1 if the argument is less than than this one, 1 if greater, and 0 if equal.
    Order(Key) int
}
```
A Key has a sort order and deterministic maximum distance from leaves.










## <a name="Mast">type</a> [Mast](/src/mast/lib.go?s=255:1049#L13)
``` go
type Mast struct {
    // contains filtered or unexported fields
}

```
Mast encapsulates data and parameters for the in-memory portion of a Merkle Search Tree.







### <a name="NewInMemory">func</a> [NewInMemory](/src/mast/pub.go?s=11208:11231#L394)
``` go
func NewInMemory() Mast
```
NewInMemory returns a new tree for use as an in-memory data structure
(i.e. that isn't intended to be remotely persisted).





### <a name="Mast.Clone">func</a> (Mast) [Clone](/src/mast/pub.go?s=12347:12382#L436)
``` go
func (m Mast) Clone() (Mast, error)
```



### <a name="Mast.Delete">func</a> (\*Mast) [Delete](/src/mast/pub.go?s=2122:2185#L61)
``` go
func (m *Mast) Delete(key interface{}, value interface{}) error
```
Delete deletes the entry with given key and value from the tree.




### <a name="Mast.DiffIter">func</a> (\*Mast) [DiffIter](/src/mast/pub.go?s=4096:4231#L130)
``` go
func (m *Mast) DiffIter(
    oldMast *Mast,
    f func(added, removed bool, key, addedValue, removedValue interface{}) (bool, error),
) error
```
DiffIter invokes the given callback for every entry that is different between this
and the given tree.




### <a name="Mast.DiffLinks">func</a> (\*Mast) [DiffLinks](/src/mast/pub.go?s=4378:4482#L139)
``` go
func (m *Mast) DiffLinks(
    oldMast *Mast,
    f func(removed bool, link interface{}) (bool, error),
) error
```
DiffLinks invokes the given callback for every node that is different between this
and the given tree.




### <a name="Mast.Get">func</a> (\*Mast) [Get](/src/mast/pub.go?s=5130:5196#L164)
``` go
func (m *Mast) Get(k interface{}, value interface{}) (bool, error)
```
Get gets the value of the entry with the given key and stores it at the given value pointer. Returns false if the tree doesn't contain the given key.




### <a name="Mast.Insert">func</a> (\*Mast) [Insert](/src/mast/pub.go?s=6206:6269#L208)
``` go
func (m *Mast) Insert(key interface{}, value interface{}) error
```
Insert adds or replaces the value for the given key.




### <a name="Mast.Iter">func</a> (\*Mast) [Iter](/src/mast/pub.go?s=8893:8958#L317)
``` go
func (m *Mast) Iter(f func(interface{}, interface{}) error) error
```
Iter iterates over the entries of a tree, invoking the given callback for every entry's key and value.




### <a name="Mast.MakeRoot">func</a> (\*Mast) [MakeRoot](/src/mast/pub.go?s=10886:10926#L384)
``` go
func (m *Mast) MakeRoot() (*Root, error)
```
MakeRoot makes a new persistent root, after ensuring all the changed nodes
have been written to the persistent store.




### <a name="Mast.Size">func</a> (Mast) [Size](/src/mast/pub.go?s=11987:12014#L417)
``` go
func (m Mast) Size() uint64
```
Size returns the number of entries in the tree.




## <a name="NodeCache">type</a> [NodeCache](/src/mast/node_cache.go?s=266:617#L9)
``` go
type NodeCache interface {
    // Add adds a freshly-persisted node to the cache.
    Add(key, value interface{})
    // Contains indicates the node with the given key has already been persisted.
    Contains(key interface{}) bool
    // Get retrieves the already-deserialized node with the given hash, if cached.
    Get(key interface{}) (value interface{}, ok bool)
}
```
NodeCache caches the immutable nodes from a remote storage source.
It is also used to avoid re-storing nodes, so care should be taken
to switch/invalidate NodeCache when the Persist is changed.







### <a name="NewNodeCache">func</a> [NewNodeCache](/src/mast/node_cache.go?s=740:777#L20)
``` go
func NewNodeCache(size int) NodeCache
```
NewNodeCache creates a new LRU-based node cache of the given size. One cache
can be shared by any number of trees.





## <a name="Persist">type</a> [Persist](/src/mast/pub.go?s=578:881#L21)
``` go
type Persist interface {
    // Store makes the given bytes accessible by the given name. The given string identity corresponds to the content which is immutable (never modified).
    Store(string, []byte) error
    // Load retrieves the previously-stored bytes by the given name.
    Load(string) ([]byte, error)
}
```
Persist is the interface for loading and storing (serialized) tree nodes. The given string identity corresponds to the content which is immutable (never modified).







### <a name="NewInMemoryStore">func</a> [NewInMemoryStore](/src/mast/in_memory_store.go?s=190:221#L12)
``` go
func NewInMemoryStore() Persist
```
NewInMemoryStore provides a Persist that stores serialized nodes in a map, usually for testing.





## <a name="RemoteConfig">type</a> [RemoteConfig](/src/mast/pub.go?s=944:1857#L29)
``` go
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

    // UnmarshalerUsesRegisteredTypes indicates that the unmarshaler will know how to deserialize an interface{} for a key/value in an entry.  By default, JSON decoding doesn't do this, so is done in two stages, the first to a JsonRawMessage, the second to the actual key/value type.
    UnmarshalerUsesRegisteredTypes bool

    // NodeCache caches deserialized nodes and may be shared across multiple trees.
    NodeCache NodeCache
}

```
RemoteConfig controls how nodes are persisted and loaded.










## <a name="Root">type</a> [Root](/src/mast/pub.go?s=1950:2052#L53)
``` go
type Root struct {
    Link         *string
    Size         uint64
    Height       uint8
    BranchFactor uint
}

```
Root identifies a version of a tree whose nodes are accessible in the persistent store.







### <a name="NewRoot">func</a> [NewRoot](/src/mast/pub.go?s=11660:11740#L408)
``` go
func NewRoot(remoteOptions *CreateRemoteOptions) *Root
```
NewRoot creates an empty tree whose nodes will be persisted remotely according to remoteOptions.





### <a name="Root.LoadMast">func</a> (\*Root) [LoadMast](/src/mast/pub.go?s=9483:9542#L342)
``` go
func (r *Root) LoadMast(config RemoteConfig) (*Mast, error)
```
LoadMast loads a tree from a remote store. The root is loaded
and verified; other nodes will be loaded on demand.








- - -
Generated by [godoc2md](http://godoc.org/github.com/davecheney/godoc2md)
