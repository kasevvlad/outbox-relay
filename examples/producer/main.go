package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	customer := flag.String("customer", "alice", "customer name")
	total := flag.Int("total", 1500, "order total")
	flag.Parse()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5433/shop?sslmode=disable"
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	orderID, err := createOrder(context.Background(), db, *customer, *total)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("order %d created", orderID)
}

func createOrder(ctx context.Context, db *sql.DB, customer string, total int) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var orderID int64
	err = tx.QueryRowContext(ctx,
		`INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id`,
		customer, total,
	).Scan(&orderID)
	if err != nil {
		return 0, err
	}

	payload, err := json.Marshal(map[string]any{
		"order_id": orderID,
		"customer": customer,
		"total":    total,
	})
	if err != nil {
		return 0, err
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES ($1, $2, $3, $4)`,
		"order", strconv.FormatInt(orderID, 10), "order.created", string(payload),
	)
	if err != nil {
		return 0, err
	}

	return orderID, tx.Commit()
}
