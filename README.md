![Go](https://github.com/jrhy/mast/workflows/Go/badge.svg)
[![GoDoc](https://godoc.org/github.com/jrhy/mast?status.svg)](https://godoc.org/github.com/jrhy/mast)

# mast
immutable, versioned, diffable map implementation of the Merkle Search Tree

`import "github.com/jrhy/mast"`

# Primary use cases
* Strongly-consistent versioned KV store layer with flexible backends (S3, files today, designed for Dynamo, Firebase, SQL as well)
* Provides consistent access to multiple versions of collections or materialized views, with incremental storage cost logarithmically proportional to delta size
* Flexible change reporting through efficient diffing of snapshots


# What's new?

* v1.2.23 adds `func (m *Mast) StartDiff(context.Context,        oldMast *Mast) (*DiffCursor, error)`
for stateful iterating through differences without callbacks

# Documentation

See Go package documentation at:

https://godoc.org/github.com/jrhy/mast

