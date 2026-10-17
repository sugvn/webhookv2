package main

import (
	"flag"
	"log"
	"net/http"
	"sync/atomic"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	failN := flag.Int("fail", 0, "answer the first N requests with 500")
	flag.Parse()

	var n atomic.Int32
	http.HandleFunc("/hook", func(w http.ResponseWriter, r *http.Request) {
		c := int(n.Add(1))
		log.Printf("req=%d delivery=%s attempt=%s event=%s",
			c, r.Header.Get("X-Delivery-Id"), r.Header.Get("X-Delivery-Attempt"), r.Header.Get("X-Event-Id"))
		if c <= *failN {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	log.Fatal(http.ListenAndServe(*addr, nil))
}
