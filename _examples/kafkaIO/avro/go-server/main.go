package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/avro"
	"github.com/estuary/dekaf"
)

func records(start time.Time) dekaf.RecordsAvailableFn {
	// Simulates 3 messages being generate per second, via the maximimum available offset increasing
	// by 3 for every second that has passed since the server started.
	return func() int64 {
		return int64(3 * time.Since(start) / time.Second)
	}
}

var (
	host              = flag.String("host", "localhost", "Host of emulation server")
	port              = flag.Int("port", 9092, "Port to listen on")
	topic             = flag.String("topic", "game-results", "Topic to emulate results on")
	schemaRegistryUrl = flag.String("schema-registry", "http://localhost:8081", "URL for the schema registry")
)

func main() {
	flag.Parse()

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	handler, err := dekaf.NewHandler(dekaf.Config{
		Host:             *host,
		Port:             int32(*port),
		Debug:            false,
		RecordsAvailable: records(time.Now()),
		LimitedAPI:       true,
	})
	if err != nil {
		panic(err)
	}

	schemaClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(*schemaRegistryUrl))
	if err != nil {
		panic(err)
	}

	serializer, err := avro.NewGenericSerializer(
		schemaClient,
		serde.ValueSerde,
		avro.NewSerializerConfig(),
	)
	if err != nil {
		panic(err)
	}

	handler.AddTopic(*topic, avroGameResultHandler(*topic, serializer))

	server, err := dekaf.NewServer(ctx, fmt.Sprintf(":%d", *port), handler)
	if err != nil {
		panic(err)
	}
	log.Printf("server listening on %s", server.Addr().String())
	<-ctx.Done()
}

type GameResult struct {
	User       string    `json:"user"`
	Team       string    `json:"team"`
	Score      int       `json:"score"`
	FinishedAt time.Time `json:"finished_at"`
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
func avroGameResultHandler(topic string, ser *avro.GenericSerializer) func(context.Context, int64) (int64, []byte, error) {
	return func(ctx context.Context, offset int64) (int64, []byte, error) {
		record := GameResult{
			User:       users[rand.Intn(len(users))],
			Team:       teams[rand.Intn(len(teams))],
			Score:      rand.Intn(10),
			FinishedAt: time.Now().Add(5 * time.Minute),
		}

		b, err := ser.Serialize(topic, &record)
		if err != nil {
			return 0, nil, err
		}

		return offset, b, nil
	}
}
