package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shubhamc1947/go-concurrent-kv/pkg/concurrentmap"
)

// ----------- Stored Value with TTL -----------

type StoredValue struct {
	Data      []byte
	HasTTL    bool
	ExpiresAt time.Time
}

// JSON request/response format
type KVRequest struct {
	Value      string `json:"value"`
	TTLSeconds int64  `json:"ttl_seconds,omitempty"`
}

type KVResponse struct {
	Value     string     `json:"value"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// ----------- Metrics -----------

type Metrics struct {
	TotalRequests      atomic.Int64
	TotalGets          atomic.Int64
	TotalPuts          atomic.Int64
	TotalDeletes       atomic.Int64
	RateLimited        atomic.Int64
	Unauthorized       atomic.Int64
	NotFound           atomic.Int64
	CurrentlyStoredKey atomic.Int64 // approximate, not strict
}

// ----------- Rate Limiter -----------

type clientState struct {
	windowStart time.Time
	count       int
}

type RateLimiter struct {
	mu      sync.Mutex
	clients map[string]*clientState
	limit   int
	window  time.Duration
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		clients: make(map[string]*clientState),
		limit:   limit,
		window:  window,
	}
}

// Allow returns true if the request from this client is allowed.
func (rl *RateLimiter) Allow(clientID string) bool {
	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	st, ok := rl.clients[clientID]
	if !ok || now.Sub(st.windowStart) > rl.window {
		rl.clients[clientID] = &clientState{
			windowStart: now,
			count:       1,
		}
		return true
	}

	if st.count >= rl.limit {
		return false
	}

	st.count++
	return true
}

// ----------- Logging Middleware Helpers -----------

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		duration := time.Since(start)
		log.Printf("%s %s => %d (%s)", r.Method, r.URL.Path, rec.status, duration)
	})
}

// ----------- KV Server -----------

type KVServer struct {
	store           *concurrentmap.ConcurrentMap[string, StoredValue]
	metrics         *Metrics
	authToken       string
	rateLimiter     *RateLimiter
	ttlScanInterval time.Duration
}

// Middleware chain: auth -> rate limit -> handler
func (s *KVServer) withMiddlewares(next http.Handler) http.Handler {
	h := http.Handler(next)

	// Rate limiting
	h = s.rateLimitMiddleware(h)

	// Auth
	h = s.authMiddleware(h)

	// Logging
	h = loggingMiddleware(h)

	return h
}

// Auth middleware: checks X-API-Key if authToken is set
func (s *KVServer) authMiddleware(next http.Handler) http.Handler {
	if s.authToken == "" {
		// No auth required
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip health & metrics for easier monitoring if you want
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}

		token := r.Header.Get("X-API-Key")
		if token == "" {
			token = r.Header.Get("Authorization") // optional secondary header
		}

		if token != s.authToken && token != "Bearer "+s.authToken {
			s.metrics.Unauthorized.Add(1)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Rate limit middleware: simple per-IP limiter
func (s *KVServer) rateLimitMiddleware(next http.Handler) http.Handler {
	if s.rateLimiter == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip health
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}

		clientIP := clientIDFromRequest(r)
		if !s.rateLimiter.Allow(clientIP) {
			s.metrics.RateLimited.Add(1)
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Client identifier: use remote IP
func clientIDFromRequest(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ----------- main -----------

func main() {
	// CLI flags
	port := flag.Int("port", 8080, "Port to listen on")
	buckets := flag.Int("buckets", 64, "Number of shards/buckets")
	authToken := flag.String("auth-token", "", "Optional static auth token (X-API-Key / Authorization)")
	rateLimit := flag.Int("rate-limit", 0, "Max requests per client per window (0 = disabled)")
	rateWindow := flag.Duration("rate-window", time.Minute, "Rate limit window duration")
	ttlScanInterval := flag.Duration("ttl-scan-interval", 5*time.Second, "TTL expiry scan interval")
	flag.Parse()

	store := concurrentmap.NewStringMap[StoredValue](*buckets)
	metrics := &Metrics{}
	var rl *RateLimiter
	if *rateLimit > 0 {
		rl = NewRateLimiter(*rateLimit, *rateWindow)
	}

	server := &KVServer{
		store:           store,
		metrics:         metrics,
		authToken:       *authToken,
		rateLimiter:     rl,
		ttlScanInterval: *ttlScanInterval,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", server.handleKV)
	mux.HandleFunc("/healthz", handleHealth)
	mux.HandleFunc("/metrics", server.handleMetrics)

	handler := server.withMiddlewares(mux)

	// Start TTL expiry worker
	go server.startExpiryWorker()

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting KV server on %s with %d buckets\n", addr, *buckets)
	if server.authToken != "" {
		log.Printf("Auth token enabled (X-API-Key / Authorization)\n")
	}
	if rl != nil {
		log.Printf("Rate limiting enabled: %d req / %s per client\n", *rateLimit, *rateWindow)
	}
	log.Printf("TTL scan interval: %s\n", server.ttlScanInterval)

	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// ----------- TTL Expiry Worker -----------

func (s *KVServer) startExpiryWorker() {
	ticker := time.NewTicker(s.ttlScanInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		var toDelete []string

		// Scan all keys and collect expired ones
		s.store.Range(func(key string, value StoredValue) bool {
			if value.HasTTL && now.After(value.ExpiresAt) {
				toDelete = append(toDelete, key)
			}
			return true
		})

		// Delete outside of Range to avoid locking issues
		for _, k := range toDelete {
			s.store.Delete(k)
		}
	}
}

// ----------- Handlers -----------

func (s *KVServer) handleKV(w http.ResponseWriter, r *http.Request) {
	s.metrics.TotalRequests.Add(1)

	key := r.URL.Path[len("/kv/"):]
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.metrics.TotalPuts.Add(1)
		s.handlePutJSON(w, r, key)
	case http.MethodGet:
		s.metrics.TotalGets.Add(1)
		s.handleGetJSON(w, r, key)
	case http.MethodDelete:
		s.metrics.TotalDeletes.Add(1)
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// PUT JSON: { "value": "...", "ttl_seconds": 60 }
func (s *KVServer) handlePutJSON(w http.ResponseWriter, r *http.Request, key string) {
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	var req KVRequest
	var stored StoredValue

	if json.Unmarshal(body, &req) == nil && req.Value != "" {
		stored.Data = []byte(req.Value)
		if req.TTLSeconds > 0 {
			stored.HasTTL = true
			stored.ExpiresAt = time.Now().Add(time.Duration(req.TTLSeconds) * time.Second)
		}
	} else {
		// Fallback: treat raw body as value
		stored.Data = body
	}

	s.store.Set(key, stored)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	resp := KVResponse{Value: string(stored.Data)}
	if stored.HasTTL {
		resp.ExpiresAt = &stored.ExpiresAt
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// GET JSON: { "value": "...", "expires_at": "...optional..." }
func (s *KVServer) handleGetJSON(w http.ResponseWriter, r *http.Request, key string) {
	value, ok := s.store.Get(key)
	if !ok {
		s.metrics.NotFound.Add(1)
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	// Check TTL (lazy expiration)
	if value.HasTTL && time.Now().After(value.ExpiresAt) {
		s.store.Delete(key)
		s.metrics.NotFound.Add(1)
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := KVResponse{
		Value: string(value.Data),
	}
	if value.HasTTL {
		resp.ExpiresAt = &value.ExpiresAt
	}

	_ = json.NewEncoder(w).Encode(resp)
}

func (s *KVServer) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	s.store.Delete(key)
	w.WriteHeader(http.StatusNoContent)
}

// Health: simple JSON
func handleHealth(w http.ResponseWriter, r *http.Request) {
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// Metrics endpoint: /metrics
func (s *KVServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	resp := map[string]any{
		"total_requests":     s.metrics.TotalRequests.Load(),
		"total_gets":         s.metrics.TotalGets.Load(),
		"total_puts":         s.metrics.TotalPuts.Load(),
		"total_deletes":      s.metrics.TotalDeletes.Load(),
		"rate_limited":       s.metrics.RateLimited.Load(),
		"unauthorized":       s.metrics.Unauthorized.Load(),
		"not_found":          s.metrics.NotFound.Load(),
		"approx_keys_stored": "use Len() if you want exact per-scan",
	}

	_ = json.NewEncoder(w).Encode(resp)
}
