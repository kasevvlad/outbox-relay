package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	databaseURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5433/shop?sslmode=disable")
	natsURL := getEnv("NATS_URL", nats.DefaultURL)

	batchSize, err := strconv.Atoi(getEnv("BATCH_SIZE", "100"))
	if err != nil || batchSize <= 0 {
		log.Fatal("BATCH_SIZE must be a positive number")
	}
	pollInterval, err := time.ParseDuration(getEnv("POLL_INTERVAL", "1s"))
	if err != nil {
		log.Fatal("POLL_INTERVAL must be a duration like 500ms or 1s")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal("cannot connect to database: ", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatal("cannot connect to nats: ", err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := setupStream(ctx, js); err != nil {
		log.Fatal("cannot create stream: ", err)
	}

	log.Printf("relay started, batch %d, poll every %s", batchSize, pollInterval)
	NewRelay(db, js, batchSize, pollInterval).Run(ctx)
	log.Println("relay stopped")
}

func getEnv(name string, defaultValue string) string {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	return value
}
