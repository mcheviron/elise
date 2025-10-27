package main

import (
	"encoding/binary"
	"io"
	"log"
	"net"
)

const (
	// kafkaPort is the default port used by Kafka brokers.
	kafkaPort = ":9092"
	// hard-coded correlation ID required by the current stage.
	correlationID = 7
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

	var responseSent bool
	buffer := make([]byte, 1024)

	for {
		n, err := conn.Read(buffer)
		if n > 0 {
			log.Printf("received %d bytes from %s", n, peer)
			if !responseSent {
				if err := sendCorrelationResponse(conn); err != nil {
					log.Printf("failed to send response to %s: %v", peer, err)
					return
				}
				responseSent = true
			}
		}

		if err != nil {
			if err != io.EOF {
				log.Printf("connection error with %s: %v", peer, err)
			} else {
				log.Printf("client disconnected: %s", peer)
			}
			return
		}
	}
}

func sendCorrelationResponse(conn net.Conn) error {
	var payload [8]byte
	// message_size placeholder (0) — set correctly in later stages.
	binary.BigEndian.PutUint32(payload[0:4], 0)
	// correlation_id must echo the request's ID; stage requirement is constant 7.
	binary.BigEndian.PutUint32(payload[4:8], correlationID)

	_, err := conn.Write(payload[:])
	return err
}
