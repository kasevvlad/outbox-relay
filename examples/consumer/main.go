package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer, err := js.CreateOrUpdateConsumer(ctx, "EVENTS", jetstream.ConsumerConfig{
		Durable:       "printer",
		FilterSubject: "events.order.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}

	consumeContext, err := consumer.Consume(func(msg jetstream.Msg) {
		log.Printf("%s (outbox id %s): %s", msg.Subject(), msg.Headers().Get("Outbox-Id"), msg.Data())
		msg.Ack()
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumeContext.Stop()

	log.Println("waiting for order events, press Ctrl+C to stop")
	<-ctx.Done()
}
