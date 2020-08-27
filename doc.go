/*
Package mast provides an immutable, versioned, diffable map
implementation of a Merkle Search Tree (MST).  Masts can be huge
(not limited to memory).  Masts can be stored in anything, like a
filesystem, KV store, or blob store.  Masts are designed to be
safely concurrently accessed by multiple threads/hosts.  And like
Merkle DAGs, Masts are designed to be easy to cache and synchronize.

Uses

- Efficient storage of multiple versions of materialized views

- Diffing of versions integrates CDC/streaming

- Efficient copy-on-write alternative to Go builtin map


What are MSTs

Mast is an implementation of the structure described in the
awesome paper, "Merkle
Search Trees: Efficient State-Based CRDTs in Open Networks", by
Alex Auvolat and François Taïani, 2019
(https://hal.inria.fr/hal-02303490/document).

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

Concurrency

A Mast can be Clone()d for sharing between threads. Cloning creates
a new version that can evolve independently, yet shares all the
unmodified subtrees with its parent, and as such are relatively
cheap to create.

Inspiration

The immutable data types in Clojure, Haskell, ML and other functional
languages really do make it easier to "reason about" systems; easier
to test, provide a foundation to build more quickly on.

https://github.com/bodil/im-rs, "Blazing fast immutable collection
datatypes for Rust", by Bodil Stokke, is an exemplar: the diff algorithm
and use of property testing, are instructive, and Chunks and PoolRefs 
fill gaps in understanding of Rust's ownership model for library writers
coming from GC'd languages.
*/
package mast
