package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/shubhamc1947/go-concurrent-kv/pkg/concurrentmap"
)

// KVServer holds the HTTP handlers and the underlying store.
type KVServer struct {
	store *concurrentmap.ConcurrentMap[string, []byte]
}

func main() {
	// CLI flags
	port := flag.Int("port", 8080, "Port to listen on")
	buckets := flag.Int("buckets", 64, "Number of shards/buckets for the concurrent map")
	flag.Parse()

	store := concurrentmap.NewStringMap[[]byte](*buckets)
	server := &KVServer{store: store}

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", server.handleKV)  // /kv/{key}
	mux.HandleFunc("/healthz", handleHealth) // simple health check

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting KV server on %s with %d buckets\n", addr, *buckets)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// handleKV routes PUT/GET/DELETE for /kv/{key}
func (s *KVServer) handleKV(w http.ResponseWriter, r *http.Request) {
	// path is /kv/{key}
	// strip prefix "/kv/"
	key := r.URL.Path[len("/kv/"):]
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *KVServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	defer r.Body.Close()
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	s.store.Set(key, value)
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte("OK"))
}

func (s *KVServer) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	value, ok := s.store.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	// Return raw bytes as response body
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func (s *KVServer) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	s.store.Delete(key)
	w.WriteHeader(http.StatusNoContent)
}

// health endpoint: /healthz
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
