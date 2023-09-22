package haxmap

import (
	"sync"
)

/*
Originally based on https://github.com/xdg-go/zzz-slice-recycling
Copyright (C) 2019 by David A. Golden
License (Apache2): https://github.com/xdg-go/zzz-slice-recycling/blob/master/LICENSE
*/

// SlicePool is a wrapper around sync.Pool to get a slice with a given capacity.
type SlicePool[T any] struct {
	// Minimum capacity of slices returned by the pool.
	MinCap int
	// If true, slices are not zero'd before they are returned.
	// This means that slices could contain old data.
	NoReset bool
	pool    *sync.Pool
}

// NewSlicePool returns a new SlicePool object.
func NewSlicePool[T any](minCap int) *SlicePool[T] {
	return &SlicePool[T]{
		MinCap: minCap,
		pool:   &sync.Pool{},
	}
}

// Get a slice from the pool.
// The cap parameter is used only if we need to allocate a new slice; there's no guarantee a slice retrieved from the pool will have enough capacity for that.
func (sp SlicePool[T]) Get(cap int) []T {
	bp := sp.pool.Get()
	if bp == nil {
		if cap < sp.MinCap {
			cap = sp.MinCap
		}
		return make([]T, 0, cap)
	}
	buf := bp.([]T)

	// Reset the contents of the slice if needed
	if !sp.NoReset {
		var zero T
		for i := range buf {
			buf[i] = zero
		}
	}
	return buf[0:0]
}

// Put a slice back in the pool.
func (sp SlicePool[T]) Put(bs []T) {
	// The linter here complains because we're putting a slice rather than a pointer in the pool.
	// The complain is valid, because doing so does cause an allocation for the local copy of the slice header.
	// However, this is ok for us because given how we use SlicePool, we can't keep around the pointer we took out.
	// See this thread for some discussion: https://github.com/dominikh/go-tools/issues/1336
	//nolint:staticcheck
	sp.pool.Put(bs)
}

// Resize a slice, making sure that it has enough capacity for a given size.
func (sp SlicePool[T]) Resize(orig []T, size int) []T {
	if size < cap(orig) {
		return orig[0:size]
	}

	// Allocate a new slice and then discard the old one, too small, so it can be garbage collected
	temp := make([]T, size, max(size, cap(orig)*2))
	copy(temp, orig)
	return temp
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
