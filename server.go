package dekaf

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/estuary/dekaf/protocol"
)

// Server is used to handle the TCP connections, decode requests,
// defer to the handler, and encode the responses.
type Server struct {
	protocolLn   *net.TCPListener
	handler      *Handler
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
	requestCh    chan *Context
	responseCh   chan *Context
}

// NewServer creates a server using the passed handler
func NewServer(ctx context.Context, listen string, handler *Handler) (*Server, error) {

	s := &Server{
		handler:    handler,
		shutdownCh: make(chan struct{}),
		requestCh:  make(chan *Context, 1024),
		responseCh: make(chan *Context, 1024),
	}

	protocolAddr, err := net.ResolveTCPAddr("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}
	if s.protocolLn, err = net.ListenTCP("tcp", protocolAddr); err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	go func() {
	acceptLoop:
		for {
			select {
			case <-ctx.Done():
				break acceptLoop
			case <-s.shutdownCh:
				break acceptLoop
			default:
				conn, err := s.protocolLn.Accept()
				if err != nil {
					log.Printf("server: listener accept error: %v", err)
					continue
				}

				go s.handleRequest(conn)
			}
		}
	}()

	go func() {
	responseLoop:
		for {
			select {
			case <-ctx.Done():
				break responseLoop
			case <-s.shutdownCh:
				break responseLoop
			case respCtx := <-s.responseCh:
				if err := s.handleResponse(respCtx); err != nil {
					log.Printf("server: handle response error: %v", err)
				}
			}
		}
	}()

	go s.handler.Run(ctx, s.requestCh, s.responseCh)

	return s, nil

}

// Shutdown closes the service.
func (s *Server) Shutdown() error {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shutdown {
		return nil
	}

	s.shutdown = true
	close(s.shutdownCh)

	if err := s.handler.Shutdown(); err != nil {
		return err
	}
	if err := s.protocolLn.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Server) handleRequest(conn net.Conn) {
	defer conn.Close()

	for {
		p := make([]byte, 4)
		_, err := io.ReadFull(conn, p[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("server: conn read error: %s", err)
			break
		}

		size := protocol.Encoding.Uint32(p)
		if size == 0 {
			break // TODO: should this even happen?
		}

		b := make([]byte, size+4) //+4 since we're going to copy the size into b
		copy(b, p)

		if _, err = io.ReadFull(conn, b[4:]); err != nil {
			panic(err)
		}

		d := protocol.NewDecoder(b)
		header := new(protocol.RequestHeader)
		if err := header.Decode(d); err != nil {
			panic(err)
		}

		var req protocol.VersionedDecoder

		switch header.APIKey {
		case protocol.ProduceKey:
			req = &protocol.ProduceRequest{}
		case protocol.FetchKey:
			req = &protocol.FetchRequest{}
		case protocol.OffsetsKey:
			req = &protocol.OffsetsRequest{}
		case protocol.MetadataKey:
			req = &protocol.MetadataRequest{}
		case protocol.LeaderAndISRKey:
			req = &protocol.LeaderAndISRRequest{}
		case protocol.StopReplicaKey:
			req = &protocol.StopReplicaRequest{}
		case protocol.UpdateMetadataKey:
			req = &protocol.UpdateMetadataRequest{}
		case protocol.ControlledShutdownKey:
			req = &protocol.ControlledShutdownRequest{}
		case protocol.OffsetCommitKey:
			req = &protocol.OffsetCommitRequest{}
		case protocol.OffsetFetchKey:
			req = &protocol.OffsetFetchRequest{}
		case protocol.FindCoordinatorKey:
			req = &protocol.FindCoordinatorRequest{}
		case protocol.JoinGroupKey:
			req = &protocol.JoinGroupRequest{}
		case protocol.HeartbeatKey:
			req = &protocol.HeartbeatRequest{}
		case protocol.LeaveGroupKey:
			req = &protocol.LeaveGroupRequest{}
		case protocol.SyncGroupKey:
			req = &protocol.SyncGroupRequest{}
		case protocol.DescribeGroupsKey:
			req = &protocol.DescribeGroupsRequest{}
		case protocol.ListGroupsKey:
			req = &protocol.ListGroupsRequest{}
		case protocol.APIVersionsKey:
			req = &protocol.APIVersionsRequest{}
		case protocol.SaslHandshakeKey:
			req = &protocol.SaslHandshakeRequest{}
		case protocol.CreateTopicsKey:
			req = &protocol.CreateTopicRequests{}
		case protocol.DeleteTopicsKey:
			req = &protocol.DeleteTopicsRequest{}
		default:
			log.Println("***********************************************************")
			log.Printf("UNHANDLED MESSAGE: %#v", header)
			log.Println("***********************************************************")
			continue
		}

		if err := req.Decode(d, header.APIVersion); err != nil {
			log.Printf("server: %s: decode request failed: %v", header, err)
			panic(err)
		}

		ctx := context.Background()

		reqCtx := &Context{
			parent: ctx,
			header: header,
			req:    req,
			conn:   conn,
		}

		s.requestCh <- reqCtx
	}
}

func (s *Server) handleResponse(respCtx *Context) error {
	b, err := protocol.Encode(respCtx.res.(protocol.Encoder))
	if err != nil {
		return err
	}
	_, err = respCtx.conn.Write(b)
	return err
}

// Addr returns the address on which the Server is listening
func (s *Server) Addr() net.Addr {
	return s.protocolLn.Addr()
}
