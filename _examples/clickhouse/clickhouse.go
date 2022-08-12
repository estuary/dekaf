package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/estuary/dekaf"
)

/*

For this example we will send JSON encoded data into Clickhouse.

You should start the example go program first.

In your Clickhouse client, create a new table and point it towards the IP/PORT you are
going to run this example on. In this case we are using 172.18.0.1:9091 and the topic will
be clickhouse_test.

CREATE TABLE events (
	`fielda` Int64,
	`fieldb` String,
	`ts` Datetime64(9, 'UTC')
) ENGINE = Kafka SETTINGS kafka_broker_list = '172.18.0.1:9091',
kafka_topic_list = 'clickhouse_test',
kafka_group_name = 'group1',
kafka_format = 'JSONEachRow',
kafka_num_consumers = 1,
kafka_row_delimiter = '\n',
kafka_skip_broken_messages = 1;

Clickhouse takes timestamps in a variety of formats. In this case we use DateTime64 with nano-second
precision (9) and UTC timezone. We create a special type to assist with formatting.

You can verify data is coming through in Clickhouse with the SQL:

SELECT * FROM events;

Drop table when you are done with:

DROP TABLE events;

*/

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	// Create our handler and add the topic.
	handler, err := dekaf.NewHandler(dekaf.Config{
		Host:  "172.18.0.1",
		Port:  9091,
		Debug: false,
	})
	if err != nil {
		panic(err)
	}
	handler.AddTopic("clickhouse_test", clickHouseTestMessageProvider)

	// Listen on all IPs port 9091.
	server, err := dekaf.NewServer(ctx, ":9091", handler)
	if err != nil {
		panic(err)
	}
	log.Printf("server listening on %s", server.Addr().String())
	<-ctx.Done()
}

type SimpleRecord struct {
	A  int64                `json:"fielda"`
	B  string               `json:"fieldb"`
	TS ClickhouseDateTime64 `json:"ts"`
}

// Special time format to be compatible with Clickhouse.
type ClickhouseDateTime64 time.Time

// Returns a nanosecond precision unix timestamp.
func (ts ClickhouseDateTime64) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(ts).UnixNano())
}

// Used to track times.
var currentOffset int64
var i int64

func clickHouseTestMessageProvider(ctx context.Context, offset int64) (int64, []byte, error) {

	// Just write a simple record.
	var record = SimpleRecord{
		A:  i,
		B:  "B",
		TS: ClickhouseDateTime64(time.Now()),
	}
	i++

	b, err := json.Marshal(&record)
	if err != nil {
		panic(err)
	}

	defer func() {
		currentOffset += int64(len(b))
	}()

	// Just to keep it from going too fast.
	time.Sleep(50 * time.Millisecond)

	return currentOffset, b, nil

}
