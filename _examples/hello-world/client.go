package main

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {

	ctx := context.Background()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"127.0.0.1:9091"},
		GroupID: "group1",
		Topic:   "hello_world",
	})

	for {
		msg, err := reader.FetchMessage(ctx)
		if err == io.ErrClosedPipe {
			log.Println("DONE")
			return
		} else if err != nil {
			panic(err)
		}

		log.Printf("TOPIC:%s PARTITION:%d OFFSET:%d KEY:%s MESSAGE:%s ", msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		// Just to prevent massive runaway
		time.Sleep(10 * time.Millisecond)
	}
}
