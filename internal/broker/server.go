package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/mcheviron/elise/internal/metadata"
	"github.com/mcheviron/elise/internal/protocol"
)

const defaultPort = ":9092"

// Run initializes the broker components and starts serving client requests.
func Run() error {
	srv, err := NewServer(defaultPort)
	if err != nil {
		return err
	}
	return srv.Serve()
}

// Server owns the long-lived resources needed to handle client requests.
type Server struct {
	port        string
	metaManager *metadata.Manager
}

// NewServer constructs a Server bound to the provided port.
func NewServer(port string) (*Server, error) {
	mgr, err := metadata.NewManager(metadata.DefaultLogPath)
	if err != nil {
		return nil, fmt.Errorf("broker: initialize metadata manager: %w", err)
	}
	return &Server{
		port:        port,
		metaManager: mgr,
	}, nil
}

// Serve listens for incoming TCP connections and processes requests until an unrecoverable error occurs.
func (s *Server) Serve() error {
	defer func() {
		if err := s.metaManager.Close(); err != nil {
			log.Printf("broker: metadata manager close error: %v", err)
		}
	}()

	listener, err := net.Listen("tcp", s.port)
	if err != nil {
		return fmt.Errorf("broker: failed to bind to %s: %w", s.port, err)
	}
	defer listener.Close()

	log.Printf("Broker listening on %s", s.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				log.Printf("accept timeout: %v", err)
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("broker: accept error: %w", err)
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	peer := conn.RemoteAddr().String()
	log.Printf("client connected: %s", peer)

	for {
		req, err := readRequest(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("client disconnected: %s", peer)
				break
			}
			log.Printf("read request error from %s: %v", peer, err)
			break
		}

		log.Printf("received request api_key=%d api_version=%d correlation_id=%d", req.Header.APIKey, req.Header.APIVersion, req.Header.CorrelationID)

		header := protocol.ResponseHeaderV0{CorrelationID: req.Header.CorrelationID}

		switch req.Header.APIKey {
		case protocol.APIKeyAPIVersions:
			body := buildAPIVersionsResponse(req.Header.APIVersion).Encode()
			if err := sendResponse(conn, header, body); err != nil {
				log.Printf("failed to send ApiVersions response to %s: %v", peer, err)
				return
			}
		case protocol.APIKeyDescribeTopicPartitions:
			descReq, err := protocol.ParseDescribeTopicPartitionsRequest(req.Body)
			if err != nil {
				log.Printf("failed to parse DescribeTopicPartitions request: %v", err)
				return
			}
			respBody, err := s.buildDescribeTopicPartitionsResponse(descReq).Encode()
			if err != nil {
				log.Printf("failed to encode DescribeTopicPartitions response: %v", err)
				return
			}
			if err := sendResponse(conn, header, respBody); err != nil {
				log.Printf("failed to send DescribeTopicPartitions response to %s: %v", peer, err)
				return
			}
		case protocol.APIKeyFetch:
			fetchResp, err := buildFetchResponse(req.Header.APIVersion, req.Body)
			if err != nil {
				log.Printf("failed to build Fetch response: %v", err)
				return
			}
			body := fetchResp.Encode()
			if err := sendResponse(conn, header, body); err != nil {
				log.Printf("failed to send Fetch response to %s: %v", peer, err)
				return
			}
		default:
			log.Printf("unsupported api key %d; closing connection", req.Header.APIKey)
			return
		}
	}
}

func readRequest(conn net.Conn) (*protocol.Request, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return nil, err
	}

	messageSize := int32(binary.BigEndian.Uint32(sizeBuf[:]))
	if messageSize < 0 {
		return nil, fmt.Errorf("negative message size %d", messageSize)
	}

	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}

	return protocol.ParseRequest(messageSize, payload)
}

func sendResponse(conn net.Conn, header protocol.ResponseHeaderV0, body []byte) error {
	frame := protocol.EncodeResponse(header, body)
	_, err := conn.Write(frame)
	return err
}

func buildAPIVersionsResponse(requestedVersion int16) protocol.APIVersionsResponseV4 {
	errCode := protocol.ErrorCodeNone
	if !protocol.APIVersionsSupportedRange.Contains(requestedVersion) {
		errCode = protocol.ErrorCodeUnsupportedVersion
	}
	resp := protocol.APIVersionsResponseV4{
		ErrorCode:      errCode,
		ThrottleTimeMS: 0,
	}
	if errCode == protocol.ErrorCodeNone {
		resp.APIVersions = make([]protocol.APIVersionRange, len(protocol.SupportedAPIs))
		copy(resp.APIVersions, protocol.SupportedAPIs)
	}
	return resp
}

func buildFetchResponse(requestedVersion int16, payload []byte) (protocol.FetchResponseV16, error) {
	resp := protocol.FetchResponseV16{
		ThrottleTimeMS: 0,
		SessionID:      0,
	}
	if requestedVersion != 16 {
		resp.ErrorCode = protocol.ErrorCodeUnsupportedVersion
		return resp, nil
	}

	req, err := protocol.ParseFetchRequestV16(payload)
	if err != nil {
		return protocol.FetchResponseV16{}, err
	}

	resp.ErrorCode = protocol.ErrorCodeNone
	resp.Responses = make([]protocol.FetchableTopicResponse, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]protocol.FetchablePartitionResponse, 0, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			partitions = append(partitions, protocol.FetchablePartitionResponse{
				PartitionIndex:       partition.PartitionIndex,
				ErrorCode:            protocol.ErrorCodeUnknownTopicID,
				HighWatermark:        -1,
				LastStableOffset:     -1,
				LogStartOffset:       -1,
				PreferredReadReplica: -1,
			})
		}

		resp.Responses = append(resp.Responses, protocol.FetchableTopicResponse{
			TopicID:    topic.TopicID,
			Partitions: partitions,
		})
	}

	return resp, nil
}
