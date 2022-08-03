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

// Kafka Protocol Specification: https://kafka.apache.org/protocol
// Kafka API Keys and Request/Response definitions: https://kafka.apache.org/protocol#protocol_api_keys

const (
	defaultMaxMessagesPerTopic = 10
	defaultMessageWaitDeadline = 5 * time.Second
	memberGroupIDSuffix        = "-00000000-0000-0000-0000-000000000000"
)

// ClusterID we will use when talking to clients.
var ClusterID = "dekafclusterid"

type MaxOffsetFn func(time.Time) int64

// Config defines the handler config
type Config struct {
	// The Host we should tell Kafka clients to connect to.
	Host string
	// The Port we should tell Kafka clients to connect to.
	Port int32
	// The maximum number of messages we will provide per topic.
	// Defaults to 10 if not set.
	MaxMessagesPerTopic int
	// How long to wait for messages from the provider.
	// The config value will take precedence followed by the client request time
	// and finally if neither are set, will default to 5 seconds.
	MessageWaitDeadline time.Duration
	// Debug dumps message request/response.
	Debug bool
	// MaxOffset returns the maximum available offset that the server should simulate as available.
	MaxOffset MaxOffsetFn
}

// A MessageProvider function is used to provide messages for a topic. The handler will request
// a message at startOffset. The MessageProvider should return a message offset, payload and error
// to the request. If there are no more messages return io.EOF for the error. This function may block
// up until the provided context.Context cancels in which case it should return io.EOF.
type MessageProvider func(ctx context.Context, startOffset int64) (int64, []byte, error)

// Handler configuration.
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

	// Handle defaults for unset values.
	if h.config.MaxMessagesPerTopic <= 0 {
		h.config.MaxMessagesPerTopic = defaultMaxMessagesPerTopic
	}

	return h, nil
}

// Validates configuration.
func (c *Config) validate() error {
	if c.Host == "" || c.Port == 0 {
		return errors.New("invalid config")
	}
	return nil
}

// AddTopic adds a new topic to the server and registers the MessageProvider with that topic.
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
			case *protocol.MetadataRequest:
				res = h.handleMetadata(reqCtx, req)
			case *protocol.OffsetCommitRequest:
				res = h.handleOffsetCommit(reqCtx, req)
			case *protocol.OffsetFetchRequest:
				res = h.handleOffsetFetch(reqCtx, req)
			case *protocol.FindCoordinatorRequest:
				res = h.handleFindCoordinator(reqCtx, req)
			case *protocol.JoinGroupRequest:
				res = h.handleJoinGroup(reqCtx, req)
			case *protocol.HeartbeatRequest:
				res = h.handleHeartbeat(reqCtx, req)
			case *protocol.LeaveGroupRequest:
				res = h.handleLeaveGroup(reqCtx, req)
			case *protocol.SyncGroupRequest:
				res = h.handleSyncGroup(reqCtx, req)
			case *protocol.APIVersionsRequest:
				res = h.handleAPIVersions(reqCtx, req)
			default:
				log.Println("***********************************************************")
				log.Printf("UNHANDLED REQUEST: %#v", req)
				log.Println("***********************************************************")
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

// Shutdown the handler.
func (h *Handler) Shutdown() error {
	return nil
}

// API Versions request sent by server to see what API's are available.
func (h *Handler) handleAPIVersions(ctx *Context, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {

	// Get it to force version 0 for API requests
	if req.APIVersion != 0 {
		return &protocol.APIVersionsResponse{
			APIVersion: req.APIVersion,
			ErrorCode:  35,
		}
	}

	return &protocol.APIVersionsResponse{
		APIVersion:   req.APIVersion,
		ErrorCode:    0,
		APIVersions:  protocol.APIVersions,
		ThrottleTime: 0,
	}
}

// Metadata request gets info about topics available and the brokers for the topics.
func (h *Handler) handleMetadata(ctx *Context, req *protocol.MetadataRequest) *protocol.MetadataResponse {

	h.RLock()
	defer h.RUnlock()

	var topicMetadata []*protocol.TopicMetadata
	for _, topicName := range req.Topics {
		if _, ok := h.topics[topicName]; !ok {
			continue
		}

		topicMetadata = append(topicMetadata, &protocol.TopicMetadata{
			Topic:          topicName,
			TopicErrorCode: 0,
			PartitionMetadata: []*protocol.PartitionMetadata{
				{
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
			{
				NodeID: 1,
				Host:   h.config.Host,
				Port:   h.config.Port,
			},
		},
		ControllerID:  1,
		ClusterID:     &ClusterID,
		TopicMetadata: topicMetadata,
	}
}

// Offset request gets info about topic available messages and offsets.
func (h *Handler) handleOffsets(ctx *Context, req *protocol.OffsetsRequest) *protocol.OffsetsResponse {

	h.RLock()
	defer h.RUnlock()

	var offsetRespones []*protocol.OffsetResponse
	for _, reqTopic := range req.Topics {
		if _, ok := h.topics[reqTopic.Topic]; !ok {
			continue
		}
		var offset int64
		var ts time.Time
		if reqTopic.Partitions[0].Timestamp == -2 {
			// Earliest = 0/Epoch
			offset = 0
			ts = time.Unix(0, 0)
		} else if reqTopic.Partitions[0].Timestamp == -1 {
			// Latest = all the data up until now
			ts = time.Now()
			if h.config.MaxOffset != nil {
				offset = h.config.MaxOffset(ts)
			} else {
				offset = math.MaxInt64 // Unlimited data
			}
		}

		offsetRespones = append(offsetRespones, &protocol.OffsetResponse{
			Topic: reqTopic.Topic,
			PartitionResponses: []*protocol.PartitionResponse{
				{
					Partition: 0,
					ErrorCode: 0,
					Timestamp: ts,
					Offset:    offset,
					Offsets:   []int64{offset},
				},
			},
		})
	}
	return &protocol.OffsetsResponse{
		APIVersion:   req.APIVersion,
		ThrottleTime: 0,
		Responses:    offsetRespones,
	}
}

// OffsetFetch returns the last committed offset value for the topic.
// We may need to update this value in response to the OffsetCommit request.
func (h *Handler) handleOffsetFetch(ctx *Context, req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {

	h.RLock()
	defer h.RUnlock()
	var emptyString string

	var offsetFetchTopicResponse []protocol.OffsetFetchTopicResponse
	for _, reqTopic := range req.Topics {
		if _, ok := h.topics[reqTopic.Topic]; !ok {
			continue
		}

		offsetFetchTopicResponse = append(offsetFetchTopicResponse, protocol.OffsetFetchTopicResponse{
			Topic: reqTopic.Topic,
			Partitions: []protocol.OffsetFetchPartition{
				{
					Partition: 0,
					ErrorCode: 0,
					Metadata:  &emptyString,
					Offset:    -1, // None
				},
			},
		})

	}

	return &protocol.OffsetFetchResponse{
		APIVersion: req.APIVersion,
		Responses:  offsetFetchTopicResponse,
	}

}

// OffsetCommit sets the last committed offset value.
func (h *Handler) handleOffsetCommit(ctx *Context, req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {

	var offsetCommitTopicResponse []protocol.OffsetCommitTopicResponse
	for _, reqTopic := range req.Topics {
		if _, ok := h.topics[reqTopic.Topic]; !ok {
			continue
		}

		offsetCommitTopicResponse = append(offsetCommitTopicResponse, protocol.OffsetCommitTopicResponse{
			Topic: reqTopic.Topic,
			PartitionResponses: []protocol.OffsetCommitPartitionResponse{
				{
					Partition: 0,
					ErrorCode: 0,
				},
			},
		})
	}

	return &protocol.OffsetCommitResponse{
		APIVersion:   req.APIVersion,
		ThrottleTime: 0,
		Responses:    offsetCommitTopicResponse,
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

// Join Group asks to join a group.
func (h *Handler) handleJoinGroup(ctx *Context, req *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {

	var protoMetadata []byte
	if len(req.GroupProtocols) > 0 {
		protoMetadata = req.GroupProtocols[0].ProtocolMetadata
	}

	return &protocol.JoinGroupResponse{
		APIVersion:    req.APIVersion,
		ThrottleTime:  0,
		ErrorCode:     0,
		GenerationID:  1,
		GroupProtocol: "range",
		LeaderID:      ctx.header.ClientID + memberGroupIDSuffix,
		MemberID:      ctx.header.ClientID + memberGroupIDSuffix,
		Members: []protocol.Member{
			{
				MemberID:       ctx.header.ClientID + memberGroupIDSuffix,
				MemberMetadata: protoMetadata,
			},
		},
	}
}

// Sync Group asks to sync a group which basically will tell the client that it is the main consumer for the group.
func (h *Handler) handleSyncGroup(ctx *Context, req *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {

	var memberAssignment []byte
	if len(req.GroupAssignments) > 0 {
		memberAssignment = req.GroupAssignments[0].MemberAssignment
	}

	return &protocol.SyncGroupResponse{
		APIVersion:       req.APIVersion,
		ThrottleTime:     0,
		ErrorCode:        0,
		MemberAssignment: memberAssignment,
	}
}

// Heartbeat asks to heartbeat a group.
func (h *Handler) handleHeartbeat(ctx *Context, req *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {

	return &protocol.HeartbeatResponse{
		APIVersion:   req.APIVersion,
		ThrottleTime: 0,
		ErrorCode:    0,
	}
}

// Leave Group asks to leave a group.
func (h *Handler) handleLeaveGroup(ctx *Context, req *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	return &protocol.LeaveGroupResponse{
		APIVersion:   req.APIVersion,
		ThrottleTime: 0,
		ErrorCode:    0,
	}
}

// Fetch data handles returning data for the requested topics.
func (h *Handler) handleFetch(ctx *Context, req *protocol.FetchRequest) *protocol.FetchResponse {

	h.RLock()
	defer h.RUnlock()

	// Setup the deadline to respond.
	var deadline = h.config.MessageWaitDeadline
	if deadline == 0 {
		if req.MaxWaitTime != 0 {
			deadline = req.MaxWaitTime
		} else {
			deadline = defaultMessageWaitDeadline
		}
	}

	var deadlineCtx, deadlineCancel = context.WithDeadline(ctx.parent, time.Now().Add(deadline))
	defer deadlineCancel()

	// Capture the time of this request so that we can calculate the max offset that should be
	// available to any consumer.
	maxOffsetAvailable := int64(math.MaxInt64)
	if h.config.MaxOffset != nil {
		maxOffsetAvailable = h.config.MaxOffset(time.Now())
	}

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

			// If an offset is being requested that we don't yet have available, a partition
			// response with no records should be returned.
			startingOffset := fetchTopic.Partitions[0].FetchOffset
			if startingOffset > maxOffsetAvailable {
				responseChan <- &protocol.FetchTopicResponse{
					Topic: fetchTopic.Topic,
					PartitionResponses: []*protocol.FetchPartitionResponse{
						{
							Partition:           0,
							ErrorCode:           0,
							HighWatermark:       math.MaxInt64,
							LastStableOffset:    math.MaxInt64,
							AbortedTransactions: nil,
							RecordSet:           nil,
						},
					},
				}
				return
			}

			// Build RecordSet to respond to this topic.
			var buf bytes.Buffer
			for x := 0; x < h.config.MaxMessagesPerTopic; x++ {
				// Each message in the RecordSet batch is from the "next" offset as we build the
				// batch.
				thisOffset := startingOffset + int64(x)

				// If we've exceeded the offsets for which there are messages available, there are
				// no more available messages.
				if thisOffset > maxOffsetAvailable {
					break
				}

				offset, data, err := mp(deadlineCtx, thisOffset)
				if err == io.EOF {
					// No more available messages.
					break
				} else if err != nil {
					log.Printf("topic %s message provider error: %v", fetchTopic.Topic, err)
				}

				ms := protocol.MessageSet{
					Offset: offset,
					Message: &protocol.Message{
						Value: data,
					},
				}

				// Use the v1 message format for Fetch v2 or later. Fetch v2 supports either the v0
				// or v1 message format, but for simplicitly we will always simulate a v1 message on
				// v2+ protocols. See
				// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets.
				if req.APIVersion >= 2 {
					// 0 is for v0 message format; 1 is for v1 message format.
					ms.Message.MagicByte = 1
					ms.Message.Timestamp = time.Now()
					// Set log.message.timestamp.type = LogAppendTime, see
					// https://kafka.apache.org/documentation/#messageset
					ms.Message.Attributes = 0b1000
				}

				b, err := protocol.Encode(&ms)
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
					{
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

	// Get the topic responses and append them to the message.
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
