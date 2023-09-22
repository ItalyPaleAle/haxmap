package haxmap

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"
)

func TestCache(t *testing.T) {
	const (
		cacheCount = 5
		bucketSize = 5
	)

	clock := &clocktesting.FakeClock{}
	clock.SetTime(time.Now())

	cache := newCacheWithClock[string](cacheCount, clock, bucketSize)

	for i := 0; i < cacheCount; i++ {
		c := int(cache.s[i].cap.Load())
		l := len(cache.s[i].elems)
		require.Equalf(t, bucketSize, c, "Slice %d has unexpected cap %d", i, c)
		require.Equalf(t, l, c, "Slice %d has cap %d and len(elems) %d", i, c, l)
	}

	cache.Set("key1", "val1", 3)
	cache.Set("key2", "val2", 2)

	v, ok := cache.Get("key1")
	require.True(t, ok)
	require.Equal(t, v, "val1")

	v, ok = cache.Get("key2")
	require.True(t, ok)
	require.Equal(t, v, "val2")

	require.Equal(t, 2, int(cache.m.Len()))

	t.Run("verify bucket.num", func(t *testing.T) {
		var num int
		found := []int{}
		for i := 0; i < cacheCount; i++ {
			n := int(cache.s[i].num.Load())
			require.LessOrEqualf(t, n, 1, "Bucket %d has %d num", i, n)
			num += n
			found = append(found, i)
		}
		require.Equal(t, 2, num)
		require.Truef(t, found[1] == (found[0]+1)%cacheCount, "Buckets do not follow each-other: %v", found)
	})

	ttl1Idx := -1
	t.Run("verify grows", func(t *testing.T) {
		for i := 3; i < (3*bucketSize + 2); i++ {
			cache.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i), 1)
		}

		caps := make(map[int]int, cacheCount)
		for i := 0; i < cacheCount; i++ {
			c := int(cache.s[i].cap.Load())
			l := len(cache.s[i].elems)
			require.Equalf(t, c, l, "Slice %d has cap %d and len(elems) %d", i, c, l)
			caps[c]++
			if c > bucketSize {
				ttl1Idx = i
			}
		}

		require.Equal(t, (3*bucketSize + 1), int(cache.m.Len()))

		expect := map[int]int{
			bucketSize:     (cacheCount - 1),
			bucketSize * 3: 1,
		}
		require.Len(t, caps, 2)
		require.Equal(t, expect, caps)
		require.GreaterOrEqual(t, ttl1Idx, 0)
	})

	t.Run("do not return expired entries", func(t *testing.T) {
		if ttl1Idx == -1 {
			t.Fatalf("test 'verify grows' must be executed first")
		}

		clock.Step(time.Second)

		// key1 had TTL 3s
		v, ok := cache.Get("key1")
		require.True(t, ok)
		require.Equal(t, v, "val1")

		// key3 had TTL 1s
		_, ok = cache.Get("key3")
		require.False(t, ok)

		// Slice is still there since no set operation happened
		require.EqualValues(t, (bucketSize*3)-1, cache.s[ttl1Idx].num.Load())
		require.LessOrEqual(t, cache.s[ttl1Idx].exp.Load(), clock.Now().Unix())

		// Stale data is still in the map too
		require.Equal(t, (3*bucketSize + 1), int(cache.m.Len()))
	})

	t.Run("cleanup expired entries", func(t *testing.T) {
		if ttl1Idx == -1 {
			t.Fatalf("test 'verify grows' must be executed first")
		}

		// Before a Set operation, the stale data is still present
		beforeExp := cache.s[ttl1Idx].exp.Load()
		beforeNum := cache.s[ttl1Idx].num.Load()
		beforeLen := len(cache.s[ttl1Idx].elems)

		require.EqualValues(t, (bucketSize*3)-1, beforeNum)
		require.Equal(t, bucketSize*3, beforeLen)

		// Set cacheCount as TTL which should make the element take over the slot that was expired
		cache.Set("cleanup", "cleanup", cacheCount)

		afterExp := cache.s[ttl1Idx].exp.Load()
		afterNum := cache.s[ttl1Idx].num.Load()
		afterLen := len(cache.s[ttl1Idx].elems)
		require.Equal(t, beforeExp+cacheCount, afterExp)
		require.Equal(t, int32(1), afterNum)
		require.Equal(t, beforeLen, afterLen)

		require.Eventuallyf(t, func() bool {
			return cache.m.Len() == 3
		}, time.Second, 10*time.Millisecond, "expired records are never removed from the map; current count is %d", cache.m.Len())
	})
}

func TestCacheSetConcurrency(t *testing.T) {
	const (
		cacheCount = 5
		bucketSize = 8
		iter       = 300
	)

	clock := &clocktesting.FakeClock{}
	clock.SetTime(time.Now())

	cache := newCacheWithClock[string](cacheCount, clock, bucketSize)

	// Add elements to the cache
	wg := sync.WaitGroup{}
	wg.Add(cacheCount * iter)
	for i := 1; i <= cacheCount; i++ {
		for j := 1; j <= iter; j++ {
			go func(i, j int) {
				defer wg.Done()
				cache.Set(fmt.Sprintf("key-%d-%d", i, j), fmt.Sprintf("val-%d-%d", i, j), int64(i))
			}(i, j)
		}
	}
	wg.Wait()

	// Sleep 1s to allow things to settle, especially in the haxmap
	time.Sleep(time.Second)

	// Check that all values are set
	for i := 1; i < cacheCount; i++ {
		for j := 1; j <= iter; j++ {
			val, ok := cache.Get(fmt.Sprintf("key-%d-%d", i, j))
			require.Truef(t, ok, "key-%d-%d", i, j)
			require.Equalf(t, fmt.Sprintf("val-%d-%d", i, j), val, "key-%d-%d", i, j)
		}
	}

	// Check the lengths
	for i := 0; i < cacheCount; i++ {
		require.EqualValuesf(t, iter, cache.s[i].num.Load(), "iteration %d", i)
	}
}

func TestCacheSetConcurrencyWithTimeAdvancing(t *testing.T) {
	const (
		cacheCount = 5
		bucketSize = 8
		iter       = 300
	)

	clock := &clocktesting.FakeClock{}
	clock.SetTime(time.Now())

	cache := newCacheWithClock[string](cacheCount, clock, bucketSize)

	setFn := func(iterStart, iterEnd int) {
		wg := sync.WaitGroup{}
		wg.Add(cacheCount * (iterEnd - iterStart + 1))
		for i := 1; i <= cacheCount; i++ {
			for j := iterStart; j <= iterEnd; j++ {
				go func(i, j int) {
					defer wg.Done()
					cache.Set(fmt.Sprintf("key-%d-%d", i, j), fmt.Sprintf("val-%d-%d", i, j), int64(i))
				}(i, j)
			}
		}
		wg.Wait()
	}

	// Add 1/3 of the elements to the cache at a time, advancing the clock every time (and sleeping 200ms every time to let things settle)
	setFn(1, iter/3)
	time.Sleep(200 * time.Millisecond)
	clock.Step(time.Second)

	setFn(iter/3+1, iter/3*2)
	clock.Step(time.Second)
	time.Sleep(200 * time.Millisecond)

	setFn(iter/3*2+1, iter)
	time.Sleep(time.Second)

	// Check that all values are set
	for i := 1; i < cacheCount; i++ {
		var min int
		switch i {
		case 1:
			min = iter/3*2 + 1
		case 2:
			min = iter/3 + 1
		default:
			min = 1
		}
		for j := 1; j <= iter; j++ {
			val, ok := cache.Get(fmt.Sprintf("key-%d-%d", i, j))
			if j >= min {
				require.Truef(t, ok, "key-%d-%d", i, j)
				require.Equalf(t, fmt.Sprintf("val-%d-%d", i, j), val, "key-%d-%d", i, j)
			} else {
				require.Falsef(t, ok, "key-%d-%d", i, j)
			}
		}
	}
}
