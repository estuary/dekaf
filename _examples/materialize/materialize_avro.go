package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/estuary/dekaf"
	"github.com/hamba/avro"
)

/*

For this example we will send Avro encoded data into Materialize using the Debezium envelop
format.

You should start the example go program first.

In your Materialize instance, create a new source and point it towards the IP/PORT you are
going to run this example on. In this case we are using 172.18.0.1:9091 and the topic will
be materialize_test.

CREATE SOURCE events FROM KAFKA BROKER '172.18.0.1:9091' TOPIC 'materialize_test' FORMAT AVRO USING SCHEMA '
{
	"type": "record",
	"name": "envelope",
	"fields": [
		{
		"name": "before",
		"type": [
			{
			"name": "row",
			"type": "record",
			"fields": [
				{"name": "fielda", "type": "long"},
				{"name": "fieldb", "type": "string"},
				{"name": "ts", "type": "long", "logicalType": "timestamp-millis"}
			]
			},
			"null"
		]
		},
		{ "name": "after", "type": ["row", "null"] }
	]
}' WITH (confluent_wire_format = false) ENVELOPE DEBEZIUM;

You can verify data is coming through in Materialize with the command:

COPY (TAIL events) TO stdout;

Create an example aggregation in Materialize with:

CREATE MATERIALIZED VIEW events_view AS SELECT AVG(fielda) as avga, COUNT(fielda) as cnt, fieldb
FROM events GROUP BY fieldb;

Verify the view with:

SELECT * FROM events_view;

Drop both source and view when you are done with:

DROP VIEW events_view;
DROP SOURCE events;

*/

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	// Create our handler
	handler, err := dekaf.NewHandler(dekaf.Config{
		Host:  "172.18.0.1",
		Port:  9091,
		Debug: false,
	})
	if err != nil {
		panic(err)
	}
	handler.AddTopic("materialize_test", avroTestMessageProvider)

	// Listen on 9091
	server, err := dekaf.NewServer(ctx, ":9091", handler)
	if err != nil {
		panic(err)
	}
	log.Printf("server listening on %s", server.Addr().String())
	<-ctx.Done()
}

var schema = avro.MustParse(`{
	"type": "record",
	"name": "envelope",
	"fields": [
		{
		"name": "before",
		"type": [
			{
			"name": "row",
			"type": "record",
			"fields": [
				{"name": "fielda", "type": "long"},
				{"name": "fieldb", "type": "string"},
				{"name": "ts", "type": "long", "logicalType": "timestamp-millis"}
			]
			},
			"null"
		]
		},
		{ "name": "after", "type": ["row", "null"] }
	]
}`)

type SimpleRecord struct {
	A  int64  `avro:"fielda"`
	B  string `avro:"fieldb"`
	TS int64  `avro:"ts"`
}

type Change struct {
	Before *SimpleRecord `avro:"before"`
	After  *SimpleRecord `avro:"after"`
}

var currentOffset int64
var i int64

func avroTestMessageProvider(ctx context.Context, offset int64) (int64, []byte, error) {

	var record Change
	record.After = &SimpleRecord{
		A:  i,
		B:  "B",
		TS: time.Now().Unix() * 1000,
	}
	i++

	b, err := avro.Marshal(schema, record)
	if err != nil {
		panic(err)
	}

	defer func() {
		currentOffset += int64(len(b))
	}()

	// Just to keep it from going crazy.
	time.Sleep(50 * time.Millisecond)

	return currentOffset, b, nil

}
