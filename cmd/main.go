package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/estuary/dekaf"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	// Create our handler
	handler := dekaf.NewHandler(dekaf.Config{
		Host: "172.18.0.1",
		Port: 9091,
	})
	// Listen on 9091
	server, err := dekaf.NewServer(ctx, ":9091", handler)
	if err != nil {
		panic(err)
	}
	log.Printf("server listening on %s", server.Addr().String())
	<-ctx.Done()
	server.Shutdown()
}
