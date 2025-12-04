package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/shubhamc1947/go-concurrent-kv/pkg/concurrentmap"
)

// JSON request/response format
type KVRequest struct {
	Value string `json:"value"`
}

type KVResponse struct {
	Value string `json:"value"`
}

// Custom ResponseWriter to capture status code for logging
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Logging middleware
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}

		next.ServeHTTP(rec, r)

		duration := time.Since(start)
		log.Printf("%s %s => %d (%s)", r.Method, r.URL.Path, rec.status, duration)
	})
}

// KV Server structure
type KVServer struct {
	store *concurrentmap.ConcurrentMap[string, []byte]
}

func main() {
	// CLI flags
	port := flag.Int("port", 8080, "Port to listen on")
	buckets := flag.Int("buckets", 64, "Number of shards/buckets")
	flag.Parse()

	store := concurrentmap.NewStringMap[[]byte](*buckets)
	server := &KVServer{store: store}

	// Router
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", server.handleKV)
	mux.HandleFunc("/healthz", handleHealth)

	// Apply logging middleware
	loggedMux := loggingMiddleware(mux)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting KV server on %s with %d buckets\n", addr, *buckets)

	if err := http.ListenAndServe(addr, loggedMux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// ----------- ROUTER -----------

func (s *KVServer) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/kv/"):]
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePutJSON(w, r, key)
	case http.MethodGet:
		s.handleGetJSON(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ----------- PUT (JSON) -----------

func (s *KVServer) handlePutJSON(w http.ResponseWriter, r *http.Request, key string) {
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	// Try JSON first
	var req KVRequest
	if json.Unmarshal(body, &req) == nil && req.Value != "" {
		s.store.Set(key, []byte(req.Value))
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(KVResponse{Value: req.Value})
		return
	}

	// Fallback: raw body
	s.store.Set(key, body)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(KVResponse{Value: string(body)})
}

// ----------- GET (JSON) -----------

func (s *KVServer) handleGetJSON(w http.ResponseWriter, r *http.Request, key string) {
	value, ok := s.store.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(KVResponse{Value: string(value)})
}

// ----------- DELETE -----------

func (s *KVServer) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	s.store.Delete(key)
	w.WriteHeader(http.StatusNoContent)
}

// ----------- HEALTH -----------

func handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
