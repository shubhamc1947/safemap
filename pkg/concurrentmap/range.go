package concurrentmap

// Range calls f sequentially for each key and value present in the map.
// If f returns false, Range stops the iteration early.
func (cm *ConcurrentMap[K, V]) Range(f func(key K, value V) bool) {
	for i := range cm.buckets {
		b := &cm.buckets[i]

		b.mu.RLock()
		for k, v := range b.m {
			if !f(k, v) {
				b.mu.RUnlock()
				return
			}
		}
		b.mu.RUnlock()
	}
}
