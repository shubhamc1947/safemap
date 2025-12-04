package concurrentmap

// LoadOrStore returns the existing value if present.
// Otherwise, it stores the new value and returns it.
// loaded = true → value already existed
// loaded = false → value was inserted
func (cm *ConcurrentMap[K, V]) LoadOrStore(k K, v V) (actual V, loaded bool) {
	idx := cm.bucketIndexForKey(k)
	b := &cm.buckets[idx]

	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.m[k]; ok {
		return existing, true
	}

	b.m[k] = v
	return v, false
}

// Compute applies fn atomically.
// fn(oldValue, exists) returns (newValue, keep)
// If keep = false → key is deleted
// If keep = true  → key is updated to newValue
func (cm *ConcurrentMap[K, V]) Compute(k K, fn func(old V, exists bool) (newV V, keep bool)) {
	idx := cm.bucketIndexForKey(k)
	b := &cm.buckets[idx]

	b.mu.Lock()
	defer b.mu.Unlock()

	old, exists := b.m[k]
	newVal, keep := fn(old, exists)

	if !keep {
		delete(b.m, k)
		return
	}

	b.m[k] = newVal
}
