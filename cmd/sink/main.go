package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
)

func main() {
	addr := flag.String("addr", ":9300", "listen address")
	failRate := flag.Float64("fail", 0, "fraction of requests answered with 500")
	flag.Parse()

	var received, dups, injected atomic.Int64
	var mu sync.Mutex
	seen := map[string]struct{}{}

	http.HandleFunc("POST /hook", func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		if rand.Float64() < *failRate {
			injected.Add(1)
			w.WriteHeader(500)
			return
		}
		id := r.Header.Get("X-Delivery-Id")
		mu.Lock()
		if _, ok := seen[id]; ok {
			dups.Add(1)
		} else {
			seen[id] = struct{}{}
		}
		mu.Unlock()
		w.WriteHeader(204)
	})
	http.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		u := len(seen)
		mu.Unlock()
		json.NewEncoder(w).Encode(map[string]int64{
			"received": received.Load(), "unique_deliveries": int64(u),
			"duplicates": dups.Load(), "failures_injected": injected.Load()})
	})
	log.Fatal(http.ListenAndServe(*addr, nil))
}
