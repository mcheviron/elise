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
	// kafkaPort is the default port used by Kafka brokers.
	kafkaPort = ":9092"
)

func main() {
	listener, err := net.Listen("tcp", kafkaPort)
	if err != nil {
		log.Fatalf("failed to bind to %s: %v", kafkaPort, err)
	}
	defer listener.Close()

	log.Printf("Kafka-like broker listening on %s", kafkaPort)

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

		if err := sendCorrelationResponse(conn, req.Header.CorrelationID); err != nil {
			log.Printf("failed to send response to %s: %v", peer, err)
			break
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

func sendCorrelationResponse(conn net.Conn, correlationID int32) error {
	var payload [8]byte
	// message_size placeholder (0) — set correctly in later stages.
	binary.BigEndian.PutUint32(payload[0:4], 0)
	binary.BigEndian.PutUint32(payload[4:8], uint32(correlationID))

	_, err := conn.Write(payload[:])
	return err
}
