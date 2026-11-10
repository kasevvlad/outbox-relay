package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const streamName = "EVENTS"

type Relay struct {
	db           *sql.DB
	js           jetstream.JetStream
	batchSize    int
	pollInterval time.Duration
}

type outboxMessage struct {
	ID            int64
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
}

func NewRelay(db *sql.DB, js jetstream.JetStream, batchSize int, pollInterval time.Duration) *Relay {
	return &Relay{db: db, js: js, batchSize: batchSize, pollInterval: pollInterval}
}

func setupStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       streamName,
		Subjects:   []string{"events.>"},
		Duplicates: 2 * time.Minute,
	})
	return err
}

func (r *Relay) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		count, err := r.publishBatch(context.Background())
		if err != nil {
			log.Println("relay error:", err)
		}
		if count > 0 {
			log.Printf("published %d messages", count)
		}

		if err == nil && count == r.batchSize {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(r.pollInterval):
		}
	}
}

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	messages, err := r.loadBatch(ctx, tx)
	if err != nil {
		return 0, err
	}

	publishedIDs := []int64{}
	var publishErr error

	for _, m := range messages {
		if err := r.publish(ctx, m); err != nil {
			publishErr = fmt.Errorf("publish outbox message %d: %w", m.ID, err)
			break
		}
		publishedIDs = append(publishedIDs, m.ID)
	}

	if len(publishedIDs) > 0 {
		_, err = tx.ExecContext(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, publishedIDs)
		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return len(publishedIDs), publishErr
}

func (r *Relay) loadBatch(ctx context.Context, tx *sql.Tx) ([]outboxMessage, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, r.batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	messages := []outboxMessage{}
	for rows.Next() {
		var m outboxMessage
		if err := rows.Scan(&m.ID, &m.AggregateType, &m.AggregateID, &m.EventType, &m.Payload); err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}

	return messages, rows.Err()
}

func (r *Relay) publish(ctx context.Context, m outboxMessage) error {
	msg := nats.NewMsg("events." + m.EventType)
	msg.Data = m.Payload
	msg.Header.Set("Outbox-Id", strconv.FormatInt(m.ID, 10))
	msg.Header.Set("Aggregate-Type", m.AggregateType)
	msg.Header.Set("Aggregate-Id", m.AggregateID)
	msg.Header.Set("Event-Type", m.EventType)

	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := r.js.PublishMsg(publishCtx, msg, jetstream.WithMsgID(strconv.FormatInt(m.ID, 10)))
	return err
}
