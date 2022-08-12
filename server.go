package dekaf

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/estuary/dekaf/protocol"
)

// Server is used to handle the TCP connections, decode requests,
// defer to the handler, and encode the responses.
type Server struct {
	protocolLn *net.TCPListener
	handler    *Handler
}

// NewServer creates a server using the passed handler
func NewServer(ctx context.Context, listen string, handler *Handler) (*Server, error) {
	protocolAddr, err := net.ResolveTCPAddr("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}
	ln, err := net.ListenTCP("tcp", protocolAddr)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	s := &Server{
		handler:    handler,
		protocolLn: ln,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, err := ln.Accept()
				if err != nil {
					log.Printf("server: listener accept error: %v", err)
					continue
				}

				go s.handleConn(ctx, conn)
			}
		}
	}()

	return s, nil
}

// handleConn runs in its own goroutine to sequentially process and respond to requests on a single connection.
func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

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

		res := s.handler.HandleReq(ctx, &Context{
			parent: ctx,
			header: header,
			req:    req,
			conn:   conn,
		})

		if res != nil {
			writeResponse(&protocol.Response{
				CorrelationID: header.CorrelationID,
				Body:          res,
			}, conn)
		}
	}
}

func writeResponse(res *protocol.Response, conn net.Conn) error {
	b, err := protocol.Encode(res)
	if err != nil {
		return err
	}
	_, err = conn.Write(b)
	return err
}

// Addr returns the address on which the Server is listening
func (s *Server) Addr() net.Addr {
	return s.protocolLn.Addr()
}
