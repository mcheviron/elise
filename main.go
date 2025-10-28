package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/mcheviron/boring-kafka/internal/protocol"
)

const (
	// port is the default port used by Kafka brokers.
	port = ":9092"
)

func main() {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to bind to %s: %v", port, err)
	}
	defer listener.Close()

	log.Printf("Broker listening on %s", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
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
		case protocol.APIKeyApiVersions:
			body := buildApiVersionsResponse(req.Header.APIVersion).Encode()
			if err := sendResponse(conn, header, body); err != nil {
				log.Printf("failed to send ApiVersions response to %s: %v", peer, err)
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

func buildApiVersionsResponse(requestedVersion int16) protocol.ApiVersionsResponseV4 {
	errCode := protocol.ErrorCodeNone
	if !protocol.ApiVersionsSupportedRange.Contains(requestedVersion) {
		errCode = protocol.ErrorCodeUnsupportedVersion
	}
	resp := protocol.ApiVersionsResponseV4{
		ErrorCode:      errCode,
		ThrottleTimeMS: 0,
	}
	if errCode == protocol.ErrorCodeNone {
		resp.APIVersions = make([]protocol.APIVersionRange, len(protocol.SupportedAPIs))
		copy(resp.APIVersions, protocol.SupportedAPIs)
	}
	return resp
}
