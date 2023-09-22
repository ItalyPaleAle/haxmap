package haxmap

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	kclock "k8s.io/utils/clock"
)

const defaultMinBucketSize = 20

type cacheEntry[V any] struct {
	val V
	exp time.Time
}

type Cache[V any] struct {
	m     *Map[string, cacheEntry[V]]
	s     []cacheBucket
	cap   int64
	sp    *SlicePool[uintptr]
	clock kclock.Clock
}

func NewCache[V any](cap int64) *Cache[V] {
	return newCacheWithClock[V](cap, kclock.RealClock{}, defaultMinBucketSize)
}

func newCacheWithClock[V any](cap int64, clock kclock.Clock, minBucketSize int) *Cache[V] {
	if minBucketSize < 0 || minBucketSize > (2<<32-1) {
		panic("invalid minBucketSize")
	}
	sp := NewSlicePool[uintptr](minBucketSize)
	return &Cache[V]{
		m:     New[string, cacheEntry[V]](),
		cap:   cap,
		s:     newCacheBucketSlice(cap, sp, minBucketSize),
		sp:    sp,
		clock: clock,
	}
}

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
		el := (*element[string, cacheEntry[V]])(unsafe.Pointer(swapped[i]))
		c.m.DelElement(el)
	}

	// Put the slice back in the pool
	c.sp.Put(swapped)
}

type cacheBucket struct {
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

func (b *cacheBucket) resetIfNeeded(now int64, ttl int64) (swapped []uintptr) {
	curExp := b.exp.Load()
	if curExp > now {
		// Not expired
		return nil
	}

	// Acquire a lock before resetting
	b.mu.Lock()
	defer b.mu.Unlock()

	// Update the exp value, with a CAS to check again after we acquired the lock
	if !b.exp.CompareAndSwap(curExp, now+ttl) {
		return nil
	}

	// Swap the elems slice with a fresh one
	curCount := b.num.Swap(0)
	if curCount == 0 && b.cap.Load() >= b.minSize {
		// If there was nothing in the slice already, and the slice didn't have size 0, we don't need to reset it
		return nil
	}

	curElems := b.elems
	b.elems = b.sp.Get(int(curCount))
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
