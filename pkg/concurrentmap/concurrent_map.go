package concurrentmap

import "sync"

// Hasher defines a function that hashes a key into a uint64.
type Hasher[K comparable] func(K) uint64

// bucket represents one shard of the map.
// It contains a standard Go map protected by an RWMutex.
type bucket[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// ConcurrentMap is a sharded, thread-safe map.
// Keys are distributed across buckets using the hasher function.
type ConcurrentMap[K comparable, V any] struct {
	buckets []bucket[K, V]
	hasher  Hasher[K]
}

// New creates a ConcurrentMap with numBuckets shards and a custom hasher.
func New[K comparable, V any](numBuckets int, hasher Hasher[K]) *ConcurrentMap[K, V] {
	if numBuckets <= 0 {
		panic("numBuckets must be > 0")
	}
	if hasher == nil {
		panic("hasher must not be nil")
	}

	buckets := make([]bucket[K, V], numBuckets)
	for i := range buckets {
		buckets[i].m = make(map[K]V)
	}

	return &ConcurrentMap[K, V]{
		buckets: buckets,
		hasher:  hasher,
	}
}

// NewStringMap returns a ConcurrentMap specialized for string keys.
// Uses a built-in FNV-1a hasher.
func NewStringMap[V any](numBuckets int) *ConcurrentMap[string, V] {
	return New[string, V](numBuckets, fnv64a)
}

// ----------- Core Map Operations -----------

func (cm *ConcurrentMap[K, V]) bucketIndexForKey(k K) int {
	h := cm.hasher(k)
	return int(h % uint64(len(cm.buckets)))
}

func (cm *ConcurrentMap[K, V]) Set(k K, v V) {
	idx := cm.bucketIndexForKey(k)
	b := &cm.buckets[idx]

	b.mu.Lock()
	defer b.mu.Unlock()

	b.m[k] = v
}

func (cm *ConcurrentMap[K, V]) Get(k K) (V, bool) {
	idx := cm.bucketIndexForKey(k)
	b := &cm.buckets[idx]

	b.mu.RLock()
	defer b.mu.RUnlock()

	v, ok := b.m[k]
	return v, ok
}

func (cm *ConcurrentMap[K, V]) Delete(k K) {
	idx := cm.bucketIndexForKey(k)
	b := &cm.buckets[idx]

	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.m, k)
}

func (cm *ConcurrentMap[K, V]) Len() int {
	total := 0
	for i := range cm.buckets {
		b := &cm.buckets[i]

		b.mu.RLock()
		total += len(b.m)
		b.mu.RUnlock()
	}
	return total
}

// ----------- FNV-1a String Hasher -----------

func fnv64a(s string) uint64 {
	const (
		offset64 = 1469598103934665603
		prime64  = 1099511628211
	)

	var hash uint64 = offset64
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}
	return hash
}
