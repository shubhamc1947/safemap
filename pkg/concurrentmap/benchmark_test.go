package concurrentmap

import (
	"strconv"
	"sync"
	"testing"
)

// ---------------------
// Benchmark: ConcurrentMap
// ---------------------

func BenchmarkConcurrentMapSet(b *testing.B) {
	m := NewStringMap[int](16)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k" + strconv.Itoa(i%1000)
			m.Set(key, i)
			i++
		}
	})
}

func BenchmarkConcurrentMapGet(b *testing.B) {
	m := NewStringMap[int](16)

	for i := 0; i < 1000; i++ {
		m.Set("k"+strconv.Itoa(i), i)
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k" + strconv.Itoa(i%1000)
			m.Get(key)
			i++
		}
	})
}

// ---------------------
// Benchmark: sync.Map
// ---------------------

func BenchmarkSyncMapSet(b *testing.B) {
	var m sync.Map

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k" + strconv.Itoa(i%1000)
			m.Store(key, i)
			i++
		}
	})
}

func BenchmarkSyncMapGet(b *testing.B) {
	var m sync.Map

	for i := 0; i < 1000; i++ {
		m.Store("k"+strconv.Itoa(i), i)
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k" + strconv.Itoa(i%1000)
			m.Load(key)
			i++
		}
	})
}

// ------------------------------
// Benchmark: map + RWMutex
// ------------------------------

func BenchmarkMutexMapSet(b *testing.B) {
	m := make(map[string]int)
	var mu sync.RWMutex

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k" + strconv.Itoa(i%1000)
			mu.Lock()
			m[key] = i
			mu.Unlock()
			i++
		}
	})
}

func BenchmarkMutexMapGet(b *testing.B) {
	m := make(map[string]int)
	var mu sync.RWMutex

	for i := 0; i < 1000; i++ {
		m["k"+strconv.Itoa(i)] = i
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k" + strconv.Itoa(i%1000)
			mu.RLock()
			_ = m[key]
			mu.RUnlock()
			i++
		}
	})
}
