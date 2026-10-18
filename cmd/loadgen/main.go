package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

func main() {
	url := flag.String("url", "http://localhost:8080", "webhookd base URL")
	user := flag.String("user", "bench", "producer name")
	pass := flag.String("pass", "bench-password-1", "producer password")
	etype := flag.String("type", "bench", "event type")
	n := flag.Int("n", 20000, "events to send")
	c := flag.Int("c", 32, "concurrent clients")
	flag.Parse()

	client := &http.Client{Timeout: 30 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *c, MaxConnsPerHost: *c}}

	jobs := make(chan int)
	var mu sync.Mutex
	lat := make([]time.Duration, 0, *n)
	codes := map[int]int{}
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				body := fmt.Sprintf(`{"event_id":%q,"event_type":%q,"data":{"n":%d}}`, uuid.NewString(), *etype, j)
				req, _ := http.NewRequest("POST", *url+"/events", bytes.NewBufferString(body))
				req.SetBasicAuth(*user, *pass)
				t0 := time.Now()
				resp, err := client.Do(req)
				d := time.Since(t0)
				code := 0 // 0 = transport error
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					code = resp.StatusCode
				}
				mu.Lock()
				lat = append(lat, d)
				codes[code]++
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	p := func(q float64) time.Duration { return lat[int(float64(len(lat)-1)*q)] }
	fmt.Printf("events=%d clients=%d elapsed=%s rate=%.0f/s\n", *n, *c, elapsed.Round(time.Millisecond), float64(*n)/elapsed.Seconds())
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n", p(.5), p(.95), p(.99), lat[len(lat)-1])
	fmt.Println("status codes:", codes, "(0 = transport error)")
}
