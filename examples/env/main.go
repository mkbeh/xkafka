package main

import (
	"context"
	"fmt"
	"log"

	"github.com/caarlos0/env/v11"
	"github.com/mkbeh/xkafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()

	cfg, err := env.ParseAs[xkafka.Config]()
	if err != nil {
		log.Fatalln(err)
	}

	if cfg.Brokers == "" {
		log.Fatalln("KAFKA_BROKERS is required")
	}

	if cfg.DefaultProduceTopic == "" {
		log.Fatalln("KAFKA_DEFAULT_PRODUCE_TOPIC is required")
	}

	client, err := xkafka.NewClient(
		xkafka.WithConfig(&cfg),
		xkafka.WithClientID("env-producer"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := client.Shutdown(ctx); err != nil {
			log.Println("kafka client shutdown error:", err)
		}
	}()

	record := &kgo.Record{
		Topic: cfg.DefaultProduceTopic,
		Key:   []byte("example-key"),
		Value: []byte("hello from env example"),
	}

	if err := client.ProduceSync(ctx, record); err != nil {
		log.Fatalln(err)
	}

	fmt.Printf(
		"produced message: topic=%s, key=%s, value=%s\n",
		record.Topic,
		record.Key,
		record.Value,
	)
}
