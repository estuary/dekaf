package dekaf

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/estuary/dekaf/protocol"
	"github.com/hamba/avro"
)

// Config defines the handler config
type Config struct {
	// The Host we should tell kalka clients to connect to.
	Host string
	// The Port we should tell kafka clients to connect to.
	Port int32
}

// Handler config
type Handler struct {
	Config
	topics map[string]*topic
}

type topic struct {
	BufferStartOffset int64
	BufferEndOffset   int64
	Messages          [][]byte
}

func NewHandler(config Config) *Handler {
	return &Handler{
		Config: config,
		topics: map[string]*topic{
			"tester": &topic{},
		},
	}
}

// Run starts a loop to handle requests send back responses.
func (h *Handler) Run(ctx context.Context, requests <-chan *Context, responses chan<- *Context) {
runLoop:
	for {
		select {
		case reqCtx := <-requests:
			if reqCtx == nil {
				break runLoop
			}

			var res protocol.ResponseBody

			switch req := reqCtx.req.(type) {
			case *protocol.FetchRequest:
				res = h.handleFetch(reqCtx, req)
			case *protocol.OffsetsRequest:
				res = h.handleOffsets(reqCtx, req)
			case *protocol.FindCoordinatorRequest:
				res = h.handleFindCoordinator(reqCtx, req)
			case *protocol.MetadataRequest:
				res = h.handleMetadata(reqCtx, req)
			case *protocol.APIVersionsRequest:
				res = h.handleAPIVersions(reqCtx, req)
			default:
				log.Printf("UNHANDLED KAFKA REQUEST: %#v", req)
				continue
			}

			responses <- &Context{
				parent: reqCtx,
				conn:   reqCtx.conn,
				header: reqCtx.header,
				res: &protocol.Response{
					CorrelationID: reqCtx.header.CorrelationID,
					Body:          res,
				},
			}
		case <-ctx.Done():
			break runLoop
		}
	}
}

// Shutdown the handler
func (h *Handler) Shutdown() error {
	return nil
}

// API Versions request
func (h *Handler) handleAPIVersions(ctx *Context, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {
	return &protocol.APIVersionsResponse{APIVersions: protocol.APIVersions}
}

// Metadata request (info about topics)
func (h *Handler) handleMetadata(ctx *Context, req *protocol.MetadataRequest) *protocol.MetadataResponse {

	var topicMetadata []*protocol.TopicMetadata
	for name := range h.topics {
		topicMetadata = append(topicMetadata, &protocol.TopicMetadata{
			Topic:          name,
			TopicErrorCode: 0,
			PartitionMetadata: []*protocol.PartitionMetadata{
				&protocol.PartitionMetadata{
					PartitionErrorCode: 0,
					PartitionID:        0,
					Leader:             1,
					Replicas:           []int32{1},
					ISR:                []int32{1},
				},
			},
		})
	}

	return &protocol.MetadataResponse{
		APIVersion: req.APIVersion,
		Brokers: []*protocol.Broker{
			&protocol.Broker{
				NodeID: 1,
				Host:   h.Config.Host,
				Port:   h.Config.Port,
			},
		},
		ControllerID:  1,
		TopicMetadata: topicMetadata,
	}
}

// Offset request (info about topic available data)
func (h *Handler) handleOffsets(ctx *Context, req *protocol.OffsetsRequest) *protocol.OffsetsResponse {

	var offsetRespones []*protocol.OffsetResponse
	for _, reqTopic := range req.Topics {
		if _, ok := h.topics[reqTopic.Topic]; ok {
			var offset int64
			var ts time.Time
			if reqTopic.Partitions[0].Timestamp == -2 {
				// Earliest = 0/Epoc
				offset = 0
				ts = time.Unix(0, 0)
			} else if reqTopic.Partitions[0].Timestamp == -1 {
				// Latest = all the data up until now
				offset = math.MaxInt64 // Unlimited data
				ts = time.Now()
			}

			offsetRespones = append(offsetRespones, &protocol.OffsetResponse{
				Topic: reqTopic.Topic,
				PartitionResponses: []*protocol.PartitionResponse{
					&protocol.PartitionResponse{
						Partition: 0,
						ErrorCode: 0,
						Timestamp: ts,
						// I HAVE AN UNLIMITED AMOUNT OF DATA CURRENT AS OF NOW!!!
						Offset:  offset,
						Offsets: []int64{offset},
					},
				},
			})
		}
	}
	return &protocol.OffsetsResponse{
		APIVersion:   req.APIVersion,
		ThrottleTime: 0,
		Responses:    offsetRespones,
	}
}

// FindCoordinator message
func (h *Handler) handleFindCoordinator(ctx *Context, req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	return &protocol.FindCoordinatorResponse{
		APIVersion:   req.APIVersion,
		ThrottleTime: 0,
		ErrorCode:    0,
		ErrorMessage: nil,
		Coordinator: protocol.Coordinator{
			NodeID: 1,
			Host:   h.Config.Host,
			Port:   h.Config.Port,
		},
	}
}

// Fetch data
func (h *Handler) handleFetch(ctx *Context, req *protocol.FetchRequest) *protocol.FetchResponse {

	mustEncode := func(e protocol.Encoder) []byte {
		var b []byte
		var err error
		if b, err = protocol.Encode(e); err != nil {
			panic(err)
		}
		return b
	}

	time.Sleep(250 * time.Millisecond)

	schema := avro.MustParse(`{
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

	var record Change

	record.After = &SimpleRecord{
		A:  1,
		B:  "B",
		TS: time.Now().Unix() * 1000,
	}

	b, err := avro.Marshal(schema, record)
	if err != nil {
		panic(err)
	}

	offset := req.Topics[0].Partitions[0].FetchOffset
	log.Printf("offset: %d", offset)

	return &protocol.FetchResponse{
		APIVersion:   req.APIVersion,
		ThrottleTime: 0,
		Responses: []*protocol.FetchTopicResponse{
			&protocol.FetchTopicResponse{
				Topic: "tester",
				PartitionResponses: []*protocol.FetchPartitionResponse{
					&protocol.FetchPartitionResponse{
						Partition:           0,
						ErrorCode:           0,
						HighWatermark:       math.MaxInt64,
						LastStableOffset:    math.MaxInt64,
						AbortedTransactions: nil,
						RecordSet:           mustEncode(&protocol.MessageSet{Offset: offset, Size: int32(len(b)), Messages: []*protocol.Message{{Value: b}}}),
					},
				},
			},
		},
	}

}
