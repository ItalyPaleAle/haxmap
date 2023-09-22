package haxmap

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	// Blank import for "assume-no-moving-gc" which makes the program crash if it's compiled with a Go compiler that has a GC that moves objects currently on the heap.
	// Read more below on why this is needed.
	_ "go4.org/unsafe/assume-no-moving-gc"

	kclock "k8s.io/utils/clock"
)

const defaultMinBucketSize = 20

// Cache implements a cache for short-lived objects, up to `cap` seconds (for example, 5 seconds).
// Objects are stored in a haxmap maintained internally where they can be looked up efficiently.
// Additionally, a reference to the elements is stored in a slice, one per each TTL, which allows purging entries from the cache efficiently.
type Cache[V any] struct {
	m     *Map[string, cacheEntry[V]]
	s     []cacheBucket
	cap   int64
	sp    *SlicePool[uintptr]
	clock kclock.Clock
}

// NewCache creates a new Cache with the given capacity.
// The capacity corresponds to the maximum TTL for items in the cache.
func NewCache[V any](cap int64) *Cache[V] {
	return newCacheWithClock[V](cap, kclock.RealClock{}, defaultMinBucketSize)
}

func newCacheWithClock[V any](cap int64, clock kclock.Clock, minBucketSize int) *Cache[V] {
	if minBucketSize < 0 || minBucketSize > (2<<32-1) {
		panic("invalid minBucketSize")
	}
	sp := NewSlicePool[uintptr](minBucketSize)
	sp.NoReset = true
	return &Cache[V]{
		m:     New[string, cacheEntry[V]](),
		cap:   cap,
		s:     newCacheBucketSlice(cap, sp, minBucketSize),
		sp:    sp,
		clock: clock,
	}
}

// Set an item in the cache.
// The TTL must not be larger than the capacity of the Cache.
func (c *Cache[V]) Set(key string, val V, ttl int64) {
	if ttl <= 0 {
		panic("invalid TTL: must be > 0")
	}
	if ttl > c.cap {
		panic("invalid TTL: maximum capacity is " + strconv.FormatInt(c.cap, 10))
	}

	// Add to the cache
	now := c.clock.Now()
	el := c.m.Set(key, cacheEntry[V]{val, now.Add(time.Duration(ttl) * time.Second)})

	// Get bucket index
	nowUnix := now.Unix()
	idx := (nowUnix + ttl) % c.cap

	// Reset the bucket if it has expired
	swapped := c.s[idx].resetIfNeeded(nowUnix, ttl)

	// Add to the bucket
	c.s[idx].add(uintptr(unsafe.Pointer(el)))

	// If we have items swapped out, we need to delete them in background
	if len(swapped) > 0 {
		go c.removeSwapped(swapped)
	}
}

// Get returns an item from the cache.
// Items that have expired are not returned.
func (c *Cache[V]) Get(key string) (v V, ok bool) {
	val, ok := c.m.Get(key)
	if !ok || !val.exp.After(c.clock.Now()) {
		return v, false
	}
	return val.val, true
}

func (c *Cache[V]) removeSwapped(swapped []uintptr) {
	// Remove all elements from the map
	for i := 0; i < len(swapped); i++ {
		// govet doesn't like the line below, but it's a false positive in our case
		el := (*element[string, cacheEntry[V]])(unsafe.Pointer(swapped[i]))
		c.m.DelElement(el)
	}

	// Put the slice back in the pool
	c.sp.Put(swapped)
}

// Each item in the cache is stored in a cacheEntry, which includes the value as well as its expiration time.
type cacheEntry[V any] struct {
	val V
	exp time.Time
}

// This structs is used to reference objects stored in each "bucket" in the cache, one per each expiration second.
type cacheBucket struct {
	// In here, we use a bit of "dark arts" to optimize how the GC handles this.
	// Inside "elems", we maintain a pointer to one of the elements stored in the haxmap.
	// Rather than using `[]*element`, which is a slice of actual pointers, we convert pointers to `uintptr` and store them in the slice. This is easier on the GC because slices of pointers cause the GC to scan each item, but slices of scalar types (`uintptr` is essentially an integer) are ignored.
	// We can do this safely because there's a "strong reference" to the `element` object stored in the haxmap, so there's no risk to having this "weak reference", as the GC won't remove the object as long as it's in the haxmap.
	// This is fine as long as the Go compiler doesn't move objects that are stored on the heap, changing their address. This is true as of writing and it's guaranteed by the blank import of "assume-no-moving-gc". Should that change in the future, and this package fail to build, we can update the code to use a slice of `[]*element` instead.
	elems   []uintptr
	cap     atomic.Int32
	num     atomic.Int32
	exp     atomic.Int64
	mu      sync.Mutex
	sp      *SlicePool[uintptr]
	minSize int32
}

func newCacheBucketSlice(count int64, sp *SlicePool[uintptr], minBucketSize int) []cacheBucket {
	mbs := int32(minBucketSize)
	res := make([]cacheBucket, count)
	for i := int64(0); i < count; i++ {
		res[i] = cacheBucket{
			elems:   make([]uintptr, minBucketSize),
			sp:      sp,
			minSize: mbs,
		}
		res[i].cap.Store(mbs)
	}
	return res
}

// Resets the bucket if it's expired.
// A bucket is expired when its "exp" is in the past.
func (b *cacheBucket) resetIfNeeded(now int64, ttl int64) (swapped []uintptr) {
	// Check if the bucket is expired; if not, return right away.
	curExp := b.exp.Load()
	if curExp > now {
		return nil
	}

	// Acquire a lock before resetting
	b.mu.Lock()
	defer b.mu.Unlock()

	// Update the exp value
	// We use a CAS here because if another goroutine acquired the lock before us, it may have performed the reset already.
	if !b.exp.CompareAndSwap(curExp, now+ttl) {
		return nil
	}

	// Swap the elems slice with a fresh one
	curCount := b.num.Swap(0)
	if curCount == 0 && b.cap.Load() >= b.minSize {
		// If there was nothing in the slice already, and the slice didn't have size 0, we don't need to reset it
		return nil
	}

	// Actually perform the swap, replacing elems with a "clean" slice
	// Then, return the slice with the items that are to be removed from the haxmap
	curElems := b.elems
	desiredCap := int(((curCount / b.minSize) + 1) * b.minSize) // Round up to multiples of minSize
	b.elems = b.sp.Get(int(desiredCap))
	// Set length equal to capacity
	b.elems = b.elems[0:cap(b.elems)]
	b.cap.Store(int32(len(b.elems)))
	return curElems[0:curCount]
}

func (b *cacheBucket) add(el uintptr) {
	pos := b.num.Add(1)

	// Check if we need to expand
	b.expandIfNeeded(pos)

	// Store
	b.elems[pos-1] = el
}

// Used internally to expand the capacity of the elems slice if needed.
// If the slice needs to be expanded, the bucket is locked temporarily to protect the integrity.
func (b *cacheBucket) expandIfNeeded(req int32) {
	// If we have enough capacity, nothing to do
	if req <= b.cap.Load() {
		return
	}

	// Expand and add some buffer the min capacity
	if req < b.minSize {
		req = b.minSize
	} else {
		req += b.minSize - 1
	}

	// Get a lock
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check again after acquiring the lock
	if req <= b.cap.Load() {
		return
	}

	// Grow the slice
	b.elems = b.sp.Resize(b.elems, int(req))
	b.cap.Store(int32(len(b.elems)))
}
