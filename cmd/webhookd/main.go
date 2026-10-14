package main

import (
	"context"
	"log"
	"os"

	"github.com/sugvn/webhookv2/internal/db"
)

func main() {
	ctx := context.Background()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://webhookd:webhookd@localhost:5432/webhookd"
	}
	pool, err := db.NewPool(ctx, url, 5)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		log.Fatal(err)
	}
	log.Println("migrations up to date")
}
