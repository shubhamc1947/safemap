package concurrentmap

import (
	"strconv"
	"sync"
	"testing"
)

func TestBasicOperations(t *testing.T) {
	m := NewStringMap[int](16) // NOTICE: type argument [int]

	m.Set("a", 1)
	m.Set("b", 2)

	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Fatalf("expected a=1, got %v, ok=%v", v, ok)
	}

	if v, ok := m.Get("b"); !ok || v != 2 {
		t.Fatalf("expected b=2, got %v, ok=%v", v, ok)
	}

	m.Delete("a")
	if _, ok := m.Get("a"); ok {
		t.Fatalf("expected key 'a' to be deleted")
	}
}

func TestLen(t *testing.T) {
	m := NewStringMap[int](16) // again type argument [int]

	keys := []string{"k1", "k2", "k3"}

	for i, k := range keys {
		m.Set(k, i)
	}

	if m.Len() != 3 {
		t.Fatalf("expected Len=3, got %d", m.Len())
	}

	m.Delete("k2")
	if m.Len() != 2 {
		t.Fatalf("expected Len=2, got %d", m.Len())
	}
}

func TestConcurrentAccess(t *testing.T) {
	m := NewStringMap[int](16) // AGAIN include [int]

	var wg sync.WaitGroup
	wg.Add(50)

	for g := 0; g < 50; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := "key-" + strconv.Itoa(i%10)
				m.Set(key, i)
				m.Get(key)
			}
		}(g)
	}

	wg.Wait()

	if m.Len() < 0 {
		t.Fatalf("len should never be negative")
	}
}
