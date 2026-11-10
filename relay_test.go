package main

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func setupDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	schema, err := os.ReadFile("migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`TRUNCATE outbox, orders RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}

	return db
}

func setupNATS(t *testing.T) jetstream.JetStream {
	t.Helper()

	ns, err := server.NewServer(&server.Options{Port: -1, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server did not start")
	}
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := setupStream(context.Background(), js); err != nil {
		t.Fatal(err)
	}

	return js
}

func insertEvents(t *testing.T, db *sql.DB, count int) {
	t.Helper()

	for i := 1; i <= count; i++ {
		_, err := db.Exec(
			`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES ('order', $1, 'order.created', $2)`,
			strconv.Itoa(i), `{"order_id":`+strconv.Itoa(i)+`}`,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func unpublishedCount(t *testing.T, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func streamMessages(t *testing.T, js jetstream.JetStream) []jetstream.Msg {
	t.Helper()

	ctx := context.Background()
	consumer, err := js.OrderedConsumer(ctx, streamName, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}

	batch, err := consumer.FetchNoWait(1000)
	if err != nil {
		t.Fatal(err)
	}

	messages := []jetstream.Msg{}
	for msg := range batch.Messages() {
		messages = append(messages, msg)
	}
	return messages
}

func TestRelayPublishesInOrder(t *testing.T) {
	db := setupDB(t)
	js := setupNATS(t)
	relay := NewRelay(db, js, 100, time.Second)

	insertEvents(t, db, 5)

	count, err := relay.publishBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("published %d, want 5", count)
	}
	if unpublishedCount(t, db) != 0 {
		t.Error("some rows are still unpublished")
	}

	messages := streamMessages(t, js)
	if len(messages) != 5 {
		t.Fatalf("stream has %d messages, want 5", len(messages))
	}
	for i, msg := range messages {
		wantID := strconv.Itoa(i + 1)
		if msg.Headers().Get("Outbox-Id") != wantID {
			t.Errorf("message %d has outbox id %s", i, msg.Headers().Get("Outbox-Id"))
		}
		if msg.Subject() != "events.order.created" {
			t.Errorf("subject is %s", msg.Subject())
		}
		if string(msg.Data()) != `{"order_id": `+wantID+`}` {
			t.Errorf("data is %s", msg.Data())
		}
	}

	count, _ = relay.publishBatch(context.Background())
	if count != 0 {
		t.Errorf("second run published %d messages", count)
	}
}

func TestRelayRespectsBatchSize(t *testing.T) {
	db := setupDB(t)
	js := setupNATS(t)
	relay := NewRelay(db, js, 2, time.Second)

	insertEvents(t, db, 5)

	count, _ := relay.publishBatch(context.Background())
	if count != 2 {
		t.Errorf("published %d, want 2", count)
	}
	if unpublishedCount(t, db) != 3 {
		t.Errorf("unpublished %d, want 3", unpublishedCount(t, db))
	}
}

func TestRelayKeepsMessagesWhenNATSFails(t *testing.T) {
	db := setupDB(t)
	js := setupNATS(t)
	relay := NewRelay(db, js, 100, time.Second)

	insertEvents(t, db, 3)

	if err := js.DeleteStream(context.Background(), streamName); err != nil {
		t.Fatal(err)
	}

	count, err := relay.publishBatch(context.Background())
	if err == nil {
		t.Fatal("expected an error without a stream")
	}
	if count != 0 || unpublishedCount(t, db) != 3 {
		t.Fatalf("published %d, unpublished %d", count, unpublishedCount(t, db))
	}

	if err := setupStream(context.Background(), js); err != nil {
		t.Fatal(err)
	}

	count, err = relay.publishBatch(context.Background())
	if err != nil || count != 3 {
		t.Fatalf("after recovery published %d, err %v", count, err)
	}
}

func TestRepublishedMessageIsDeduplicated(t *testing.T) {
	db := setupDB(t)
	js := setupNATS(t)
	relay := NewRelay(db, js, 100, time.Second)

	insertEvents(t, db, 3)
	relay.publishBatch(context.Background())

	if _, err := db.Exec(`UPDATE outbox SET published_at = NULL WHERE id = 2`); err != nil {
		t.Fatal(err)
	}

	count, err := relay.publishBatch(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("published %d, err %v", count, err)
	}

	if len(streamMessages(t, js)) != 3 {
		t.Error("message was published twice")
	}
}

func TestTwoRelaysDoNotTakeTheSameRows(t *testing.T) {
	db := setupDB(t)
	js := setupNATS(t)

	insertEvents(t, db, 50)

	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			relay := NewRelay(db, js, 5, time.Second)
			for {
				count, err := relay.publishBatch(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if count == 0 {
					return
				}
				mu.Lock()
				total += count
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if total != 50 {
		t.Errorf("relays published %d rows in total, want 50", total)
	}
	if unpublishedCount(t, db) != 0 {
		t.Error("some rows are still unpublished")
	}
}
