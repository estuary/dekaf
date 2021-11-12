package dekaf

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"math"
	"sync"
	"time"

	"github.com/estuary/dekaf/protocol"
)

const (
	DefaultMaxMessagesPerTopic    = 5
	DefaultMessageDeadlineSeconds = 5
)

// Config defines the handler config
type Config struct {
	// The Host we should tell Kafka clients to connect to.
	Host string
	// The Port we should tell Kafka clients to connect to.
	Port int32
	// The maximum number of messages we will provide per topic.
	// Defaults to 5 if not set.
	MaxMessagesPerTopic int
	// Debug dumps message request/response.
	Debug bool
}

// A MessageProvider function is used to provide messages for a topic. The handler will request
// a message at startOffset. The MessageProvider should return a message offset, payload and error
// to the request. If there are no more messages return io.EOF for the error. This function may block
// up until the provided context.Context cancels in which case it should return io.EOF. If a
type MessageProvider func(ctx context.Context, startOffset int64) (int64, []byte, error)

// Handler config
type Handler struct {
	config Config
	topics map[string]MessageProvider
	sync.RWMutex
}

func NewHandler(config Config) (*Handler, error) {

	if err := config.validate(); err != nil {
		return nil, err
	}

	var h = &Handler{
		config: config,
		topics: make(map[string]MessageProvider),
	}

	if h.config.MaxMessagesPerTopic == 0 {
		h.config.MaxMessagesPerTopic = DefaultMaxMessagesPerTopic
	}

	return h, nil
}

// Validates config
func (c *Config) validate() error {
	if c.Host == "" || c.Port == 0 {
		return errors.New("invalid config")
	}
	return nil
}

func (h *Handler) AddTopic(name string, mp MessageProvider) {
	h.Lock()
	h.topics[name] = mp
	h.Unlock()
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

			if h.config.Debug {
				log.Println("-----------------------------------------")
				log.Printf("REQ: %#v", reqCtx.req)
				log.Printf("RES: %#v", res)
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

// API Versions request sent by server to see what API's are available.
func (h *Handler) handleAPIVersions(ctx *Context, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {
	return &protocol.APIVersionsResponse{APIVersions: protocol.APIVersions}
}

// Metadata request gets info about topics available and the brokers for the topics.
func (h *Handler) handleMetadata(ctx *Context, req *protocol.MetadataRequest) *protocol.MetadataResponse {

	h.RLock()
	defer h.RUnlock()

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
				Host:   h.config.Host,
				Port:   h.config.Port,
			},
		},
		ControllerID:  1,
		TopicMetadata: topicMetadata,
	}
}

// Offset request gets info about topic available data.
func (h *Handler) handleOffsets(ctx *Context, req *protocol.OffsetsRequest) *protocol.OffsetsResponse {

	h.RLock()
	defer h.RUnlock()

	var offsetRespones []*protocol.OffsetResponse
	for _, reqTopic := range req.Topics {
		if _, ok := h.topics[reqTopic.Topic]; ok {
			var offset int64
			var ts time.Time
			if reqTopic.Partitions[0].Timestamp == -2 {
				// Earliest = 0/Epoch
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
						Offset:    offset,
						Offsets:   []int64{offset},
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

// FindCoordinator message gets coordinator/host information.
func (h *Handler) handleFindCoordinator(ctx *Context, req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	return &protocol.FindCoordinatorResponse{
		APIVersion:   req.APIVersion,
		ThrottleTime: 0,
		ErrorCode:    0,
		ErrorMessage: nil,
		Coordinator: protocol.Coordinator{
			NodeID: 1,
			Host:   h.config.Host,
			Port:   h.config.Port,
		},
	}
}

// Fetch data handles returning data for the requested topics.
func (h *Handler) handleFetch(ctx *Context, req *protocol.FetchRequest) *protocol.FetchResponse {

	h.RLock()
	defer h.RUnlock()

	// Setup the deadline to respond.
	var deadline = req.MaxWaitTime
	if deadline == 0 {
		deadline = time.Second * DefaultMessageDeadlineSeconds
	}
	var deadlineCtx, deadlineCancel = context.WithDeadline(ctx.parent, time.Now().Add(deadline))
	defer deadlineCancel()

	var responseChan = make(chan *protocol.FetchTopicResponse)
	for _, fetchTopic := range req.Topics {

		// Process all topics in parallel.
		go func(fetchTopic *protocol.FetchTopic) {

			// See if we have that topic message provider.
			mp, ok := h.topics[fetchTopic.Topic]
			if !ok {
				responseChan <- nil // Not found, return nothing.
				return
			}

			// Make sure we're requesting the zero partition and get the fetchOffset.
			if len(fetchTopic.Partitions) < 1 || fetchTopic.Partitions[0].Partition != 0 {
				responseChan <- nil // Invalid request, return nothing.
				log.Printf("invalid partition request: %d", fetchTopic.Partitions[0].Partition)
				return
			}
			fetchOffset := fetchTopic.Partitions[0].FetchOffset

			// Build RecordSet to respond to this topic.
			var buf bytes.Buffer
			for x := 0; x < h.config.MaxMessagesPerTopic; x++ {
				offset, data, err := mp(deadlineCtx, fetchOffset)
				if err == io.EOF {
					// No more available messages.
					break
				} else if err != nil {
					log.Printf("topic %s message provider error: %v", fetchTopic.Topic, err)
				}

				b, err := protocol.Encode(&protocol.MessageSet{
					Offset: offset,
					Message: &protocol.Message{
						Value: data,
					},
				})
				if err != nil {
					panic(err)
				}
				if _, err = buf.Write(b); err != nil {
					panic(err)
				}
			}

			// If no messages were fetched for this topic, return nothing.
			if buf.Len() == 0 {
				responseChan <- nil
			}

			responseChan <- &protocol.FetchTopicResponse{
				Topic: fetchTopic.Topic,
				PartitionResponses: []*protocol.FetchPartitionResponse{
					&protocol.FetchPartitionResponse{
						Partition:           0,
						ErrorCode:           0,
						HighWatermark:       math.MaxInt64,
						LastStableOffset:    math.MaxInt64,
						AbortedTransactions: nil,
						RecordSet:           buf.Bytes(),
					},
				},
			}

		}(fetchTopic)
	}

	var responses []*protocol.FetchTopicResponse
	for x := 0; x < len(req.Topics); x++ {
		response := <-responseChan
		if response == nil {
			continue
		}
		responses = append(responses, response)
	}

	return &protocol.FetchResponse{
		APIVersion:   req.APIVersion,
		ThrottleTime: 0,
		Responses:    responses,
	}

}
