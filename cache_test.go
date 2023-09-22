package haxmap

import (
	"fmt"
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

	t.Run("verify grows", func(t *testing.T) {
		for i := 3; i < 3*bucketSize; i++ {
			cache.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i), 1)
		}

		caps := make(map[int]int, cacheCount)
		for i := 0; i < cacheCount; i++ {
			c := int(cache.s[i].cap.Load())
			l := len(cache.s[i].elems)
			require.Equalf(t, c, l, "Slice %d has cap %d and len(elems) %d", i, c, l)
			caps[c]++
		}

		expect := map[int]int{
			bucketSize:     (cacheCount - 1),
			3 * bucketSize: 1,
		}
		require.Len(t, caps, 2)
		require.Equal(t, expect, caps)
	})

	t.Run("do not return expired entries", func(t *testing.T) {
		clock.Step(time.Second)

		// key1 had TTL 3s
		v, ok := cache.Get("key1")
		require.True(t, ok)
		require.Equal(t, v, "val1")

		// key3 had TTL 1s
		_, ok = cache.Get("key3")
		require.False(t, ok)
	})
}
