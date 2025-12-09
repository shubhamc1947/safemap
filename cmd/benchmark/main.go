package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	// 1. Configuration Flags
	target := flag.String("url", "http://localhost:8080/kv/", "Base URL of the KV server")
	totalReqs := flag.Int("n", 100000, "Total number of requests to send")
	concurrency := flag.Int("c", 100, "Number of concurrent workers")
	flag.Parse()

	fmt.Printf("🔥 Starting Benchmark: %d requests with %d workers to %s\n", *totalReqs, *concurrency, *target)

	// 2. Setup High-Performance HTTP Client
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 2000
	t.MaxConnsPerHost = 2000
	t.MaxIdleConnsPerHost = 2000

	client := &http.Client{
		Transport: t,
		Timeout:   10 * time.Second,
	}

	// 3. Metrics
	var (
		success atomic.Int64
		failed  atomic.Int64
	)

	// Payload for PUT requests
	// We create this once to reuse it, but bytes.NewReader needs to be fresh per request
	// or reset, but creating a fresh reader is cheap enough here.
	payloadData := []byte(`{"value": "benchmark_data", "ttl_seconds": 300}`)

	start := time.Now()
	var wg sync.WaitGroup

	// 4. Launch Workers
	reqsPerWorker := *totalReqs / *concurrency

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < reqsPerWorker; j++ {
				key := fmt.Sprintf("key-%d-%d", workerID, j)
				url := *target + key

				// --- Operation 1: PUT (Corrected) ---
				// Create the request
				req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(payloadData))
				if err != nil {
					failed.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				// Execute it
				resp, err := client.Do(req)
				if err != nil || resp.StatusCode != 201 {
					failed.Add(1)
					if resp != nil {
						io.Copy(io.Discard, resp.Body) // Drain anyway
						resp.Body.Close()
					}
				} else {
					success.Add(1)
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}

				// --- Operation 2: GET ---
				resp, err = client.Get(url)
				if err != nil || resp.StatusCode != 200 {
					failed.Add(1)
					if resp != nil {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				} else {
					success.Add(1)
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// 5. Calculate Results
	// Multiply by 2 because we do a PUT and a GET for every loop iteration
	totalOps := (success.Load() + failed.Load())
	rps := float64(totalOps) / duration.Seconds()

	fmt.Println("\n--- 📊 Benchmark Results ---")
	fmt.Printf("Time Taken:       %v\n", duration)
	fmt.Printf("Total Operations: %d\n", totalOps)
	fmt.Printf("Successful:       %d\n", success.Load())
	fmt.Printf("Failed:           %d\n", failed.Load())
	fmt.Println("-------------------------------")
	fmt.Printf("🚀 Requests/Sec:  %.2f\n", rps)
	fmt.Println("-------------------------------")
}
