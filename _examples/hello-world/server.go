package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/estuary/dekaf"
)

func main() {

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	// Create our handler
	handler, err := dekaf.NewHandler(dekaf.Config{
		Host:  "127.0.0.1", // This is the address the client should connect to.
		Port:  9091,        // This is the port the client should connect to.
		Debug: false,       // Will log all messages.
	})
	if err != nil {
		panic(err)
	}

	// Create a topic and pass a handler
	handler.AddTopic("hello_world", sampleHandler)

	// Create the server and listen on port 9091 and pass it the handler.
	server, err := dekaf.NewServer(ctx, ":9091", handler)
	if err != nil {
		panic(err)
	}
	log.Printf("server listening on %s", server.Addr().String())
	<-ctx.Done()
}

type SampleRecord struct {
	Offset int64
	Sample string
}

// Just return json offset.
func sampleHandler(ctx context.Context, offset int64) (int64, []byte, error) {

	record := SampleRecord{
		Offset: offset,
		Sample: fmt.Sprintf("Hello World! Offset: %d", offset),
	}

	b, err := json.Marshal(record)
	if err != nil {
		panic(err)
	}

	return offset, b, nil

}
