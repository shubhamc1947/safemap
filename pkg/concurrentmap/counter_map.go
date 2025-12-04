package concurrentmap

// CounterMap is a specialized atomic integer counter map.
// Useful for metrics, request counters, rate limits, etc.
type CounterMap[K comparable] struct {
	m *ConcurrentMap[K, int64]
}

// NewCounterMap creates a new CounterMap.
func NewCounterMap[K comparable](numBuckets int, hasher Hasher[K]) *CounterMap[K] {
	return &CounterMap[K]{
		m: New[K, int64](numBuckets, hasher),
	}
}

// NewStringCounterMap creates a counter map with string keys.
func NewStringCounterMap(numBuckets int) *CounterMap[string] {
	return NewCounterMap[string](numBuckets, fnv64a)
}

// Inc atomically increments a key by delta.
// Returns the new value.
func (cm *CounterMap[K]) Inc(k K, delta int64) int64 {
	var result int64

	cm.m.Compute(k, func(old int64, exists bool) (int64, bool) {
		if !exists {
			result = delta
			return delta, true
		}
		result = old + delta
		return result, true
	})

	return result
}

// Get returns the counter value.
func (cm *CounterMap[K]) Get(k K) (int64, bool) {
	return cm.m.Get(k)
}
