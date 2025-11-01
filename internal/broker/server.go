package broker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"

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
	logDir      string
}

// NewServer constructs a Server bound to the provided port.
func NewServer(port string) (*Server, error) {
	mgr, err := metadata.NewManager(metadata.DefaultLogPath)
	if err != nil {
		return nil, fmt.Errorf("broker: initialize metadata manager: %w", err)
	}
	logDir := filepath.Dir(filepath.Dir(metadata.DefaultLogPath))
	return &Server{
		port:        port,
		metaManager: mgr,
		logDir:      logDir,
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
			fetchResp, err := s.buildFetchResponse(req.Header.APIVersion, req.Body)
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

func (s *Server) buildFetchResponse(requestedVersion int16, payload []byte) (protocol.FetchResponseV16, error) {
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
	cluster := s.metaManager.Cluster()
	for _, topic := range req.Topics {
		var (
			metaTopic  *metadata.Topic
			topicFound bool
		)
		if cluster != nil {
			metaTopic, topicFound = cluster.TopicByID(topic.TopicID)
		}

		topicResp := protocol.FetchableTopicResponse{
			TopicID:    topic.TopicID,
			Partitions: make([]protocol.FetchablePartitionResponse, 0, len(topic.Partitions)),
		}

		for _, partition := range topic.Partitions {
			partitionResp := protocol.FetchablePartitionResponse{
				PartitionIndex:       partition.PartitionIndex,
				ErrorCode:            protocol.ErrorCodeUnknownTopicID,
				HighWatermark:        -1,
				LastStableOffset:     -1,
				LogStartOffset:       -1,
				AbortedTransactions:  []protocol.FetchResponseAbortedTransaction{},
				PreferredReadReplica: -1,
				Records:              nil,
			}

			if !topicFound || metaTopic == nil {
				topicResp.Partitions = append(topicResp.Partitions, partitionResp)
				continue
			}

			if _, ok := metaTopic.Partitions[partition.PartitionIndex]; !ok {
				partitionResp.ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
				topicResp.Partitions = append(topicResp.Partitions, partitionResp)
				continue
			}

			records, baseOffset, lastOffset, err := s.readPartitionRecords(metaTopic, partition.PartitionIndex)
			if err != nil {
				log.Printf("broker: failed to read records for topic %s partition %d: %v", metaTopic.Name, partition.PartitionIndex, err)
				partitionResp.ErrorCode = protocol.ErrorCodeUnknownTopicOrPartition
				topicResp.Partitions = append(topicResp.Partitions, partitionResp)
				continue
			}

			partitionResp.ErrorCode = protocol.ErrorCodeNone
			partitionResp.HighWatermark = lastOffset + 1
			partitionResp.LastStableOffset = lastOffset + 1
			partitionResp.LogStartOffset = baseOffset
			partitionResp.Records = records

			topicResp.Partitions = append(topicResp.Partitions, partitionResp)
		}

		resp.Responses = append(resp.Responses, topicResp)
	}

	return resp, nil
}

func (s *Server) readPartitionRecords(topic *metadata.Topic, partitionIndex int32) ([]byte, int64, int64, error) {
	if topic == nil {
		return nil, 0, 0, fmt.Errorf("nil topic metadata")
	}

	name := strings.TrimSpace(topic.Name)
	if name == "" {
		return nil, 0, 0, fmt.Errorf("topic %x has no name in metadata", topic.ID)
	}

	if s.logDir == "" {
		return nil, 0, 0, fmt.Errorf("log directory is not configured")
	}

	partitionDir := filepath.Join(s.logDir, fmt.Sprintf("%s-%d", name, partitionIndex))
	entries, err := os.ReadDir(partitionDir)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read partition log directory %q: %w", partitionDir, err)
	}

	segments := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".log") {
			segments = append(segments, entry.Name())
		}
	}
	if len(segments) == 0 {
		return nil, 0, 0, fmt.Errorf("no log segments found in %q", partitionDir)
	}

	sort.Strings(segments)
	segmentPath := filepath.Join(partitionDir, segments[len(segments)-1])
	return readFirstRecordBatch(segmentPath)
}

func readFirstRecordBatch(path string) ([]byte, int64, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("open log segment %q: %w", path, err)
	}
	defer file.Close()

	var baseOffset int64
	if err := binary.Read(file, binary.BigEndian, &baseOffset); err != nil {
		return nil, 0, 0, fmt.Errorf("read base offset: %w", err)
	}

	var batchLength int32
	if err := binary.Read(file, binary.BigEndian, &batchLength); err != nil {
		return nil, 0, 0, fmt.Errorf("read batch length: %w", err)
	}
	if batchLength < 0 {
		return nil, 0, 0, fmt.Errorf("negative batch length %d", batchLength)
	}

	payload := make([]byte, int(batchLength))
	if _, err := io.ReadFull(file, payload); err != nil {
		return nil, 0, 0, fmt.Errorf("read batch payload: %w", err)
	}

	lastOffset, err := computeLastOffset(baseOffset, payload)
	if err != nil {
		return nil, 0, 0, err
	}

	return payload, baseOffset, lastOffset, nil
}

func computeLastOffset(baseOffset int64, batch []byte) (int64, error) {
	reader := bytes.NewReader(batch)

	var partitionLeaderEpoch int32
	if err := binary.Read(reader, binary.BigEndian, &partitionLeaderEpoch); err != nil {
		return 0, fmt.Errorf("read partition leader epoch: %w", err)
	}

	magic, err := reader.ReadByte()
	if err != nil {
		return 0, fmt.Errorf("read record batch magic: %w", err)
	}
	if magic != 2 {
		return 0, fmt.Errorf("unsupported record batch magic %d", magic)
	}

	var crc int32
	if err := binary.Read(reader, binary.BigEndian, &crc); err != nil {
		return 0, fmt.Errorf("read crc: %w", err)
	}

	var attributes int16
	if err := binary.Read(reader, binary.BigEndian, &attributes); err != nil {
		return 0, fmt.Errorf("read attributes: %w", err)
	}
	compressionType := attributes & 0x7
	if compressionType != 0 {
		return 0, fmt.Errorf("compressed record batch (type %d) not supported", compressionType)
	}

	var lastOffsetDelta int32
	if err := binary.Read(reader, binary.BigEndian, &lastOffsetDelta); err != nil {
		return 0, fmt.Errorf("read last offset delta: %w", err)
	}

	// Skip fields we do not currently need but must consume to decode the batch header.
	var discard64 int64
	if err := binary.Read(reader, binary.BigEndian, &discard64); err != nil {
		return 0, fmt.Errorf("read base timestamp: %w", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &discard64); err != nil {
		return 0, fmt.Errorf("read max timestamp: %w", err)
	}

	if err := binary.Read(reader, binary.BigEndian, &discard64); err != nil {
		return 0, fmt.Errorf("read producer id: %w", err)
	}

	var discard16 int16
	if err := binary.Read(reader, binary.BigEndian, &discard16); err != nil {
		return 0, fmt.Errorf("read producer epoch: %w", err)
	}

	var discard32 int32
	if err := binary.Read(reader, binary.BigEndian, &discard32); err != nil {
		return 0, fmt.Errorf("read base sequence: %w", err)
	}

	var recordCount int32
	if err := binary.Read(reader, binary.BigEndian, &recordCount); err != nil {
		return 0, fmt.Errorf("read record count: %w", err)
	}
	if recordCount <= 0 {
		return 0, fmt.Errorf("record batch contained %d records", recordCount)
	}

	_ = partitionLeaderEpoch
	_ = crc
	return baseOffset + int64(lastOffsetDelta), nil
}
