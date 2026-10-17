package main

import (
	"flag"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	failN := flag.Int("fail", 0, "answer the first N requests with 500")
	slowN := flag.Int("slow", 0, "delay the first N requests by -delay")
	delay := flag.Duration("delay", time.Minute, "how long slow requests take")
	flag.Parse()

	var n atomic.Int32
	http.HandleFunc("/hook", func(w http.ResponseWriter, r *http.Request) {
		c := int(n.Add(1))
		log.Printf("req=%d delivery=%s attempt=%s event=%s",
			c, r.Header.Get("X-Delivery-Id"), r.Header.Get("X-Delivery-Attempt"), r.Header.Get("X-Event-Id"))
		if c <= *slowN {
			select {
			case <-r.Context().Done(): // client died
				log.Printf("req=%d: client went away", c)
				return
			case <-time.After(*delay):
			}
		}
		if c <= *failN {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	log.Fatal(http.ListenAndServe(*addr, nil))
}
