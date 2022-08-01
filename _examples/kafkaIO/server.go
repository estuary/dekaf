package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/estuary/dekaf"
)

func newMaxOffsetFn(start time.Time) dekaf.MaxOffsetFn {
	// Simulates 3 messages being generate per second, via the maximimum available offset increasing
	// by 3 for every second that has passed since the server started.
	return func(at time.Time) int64 {
		delta := at.Sub(start)
		ps := int64(delta / time.Second)
		return ps * 3
	}
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	port := 9092

	handler, err := dekaf.NewHandler(dekaf.Config{
		Host:      "127.0.0.1",
		Port:      int32(port),
		Debug:     true,
		MaxOffset: newMaxOffsetFn(time.Now()),
	})
	if err != nil {
		panic(err)
	}

	handler.AddTopic("hello_world", gameResultHandler)

	server, err := dekaf.NewServer(ctx, fmt.Sprintf(":%d", port), handler)
	if err != nil {
		panic(err)
	}
	log.Printf("server listening on %s", server.Addr().String())

	<-ctx.Done()
	server.Shutdown()
}

type GameResult struct {
	Offset     int64
	User       string
	Team       string
	Score      int
	FinishedAt time.Time
}

// Random list of user names to pick from.
var users = []string{
	"user1", "user2", "user3",
}

// Random list of team names to pick from.
var teams = []string{
	"team1", "team2", "team3", "team4", "team5", "team6",
}

// gameResultHandler will return some synthetic data representing the result of a "game" for an
// offset. The caller is responsible for making sure the offset should be simulated as existing.
func gameResultHandler(ctx context.Context, offset int64) (int64, []byte, error) {
	record := GameResult{
		Offset:     offset,
		User:       users[rand.Intn(len(users))],
		Team:       teams[rand.Intn(len(teams))],
		Score:      rand.Intn(10),
		FinishedAt: time.Now().Add(5 * time.Minute),
	}

	b, err := json.Marshal(record)
	if err != nil {
		panic(err)
	}
	return offset, b, nil

}
