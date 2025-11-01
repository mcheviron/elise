package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:9092", "broker address")
		topicIDRaw  = flag.String("topic-id", "", "topic UUID (hex or canonical)")
		partition   = flag.Int("partition", 0, "partition index to fetch")
		correlation = flag.Int("correlation", 99, "correlation id to use")
		timeout     = flag.Duration("timeout", 3*time.Second, "socket timeout")
	)
	flag.Parse()

	if strings.TrimSpace(*topicIDRaw) == "" {
		fmt.Fprintln(os.Stderr, "missing -topic-id")
		flag.Usage()
		os.Exit(2)
	}

	topicID, err := parseUUID(*topicIDRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid topic id: %v\n", err)
		os.Exit(2)
	}

	if err := run(*addr, topicID, int32(*partition), int32(*correlation), *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "fetch-empty-test: %v\n", err)
		os.Exit(1)
	}
}

func run(addr string, topicID [16]byte, partition int32, correlationID int32, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	frame := buildFetchRequestFrame(topicID, partition, correlationID)
	if _, err := conn.Write(frame); err != nil {
		return fmt.Errorf("write request: %w", err)
	}

	var sizePrefix [4]byte
	if _, err := io.ReadFull(conn, sizePrefix[:]); err != nil {
		return fmt.Errorf("read response size: %w", err)
	}
	messageSize := int(binary.BigEndian.Uint32(sizePrefix[:]))
	if messageSize <= 0 {
		return fmt.Errorf("invalid response size %d", messageSize)
	}

	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return fmt.Errorf("read response payload: %w", err)
	}

	return verifyFetchResponse(payload, correlationID, topicID, partition)
}

func buildFetchRequestFrame(topicID [16]byte, partition int32, correlationID int32) []byte {
	body := buildFetchRequestBody(topicID, partition)

	header := bytes.NewBuffer(make([]byte, 0, 32))
	writeInt16(header, 1)  // API Key Fetch
	writeInt16(header, 16) // version
	writeInt32(header, correlationID)
	writeCompactNullableString(header, nil)
	writeUVarInt(header, 0) // no header tagged fields

	out := bytes.NewBuffer(make([]byte, 0, header.Len()+len(body)+4))
	writeInt32(out, int32(header.Len()+len(body)))
	out.Write(header.Bytes())
	out.Write(body)
	return out.Bytes()
}

func buildFetchRequestBody(topicID [16]byte, partition int32) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	writeInt32(buf, 100)     // max_wait_ms
	writeInt32(buf, 1)       // min_bytes
	writeInt32(buf, 1048576) // max_bytes
	buf.WriteByte(0)         // isolation_level
	writeInt32(buf, 0)       // session_id
	writeInt32(buf, 0)       // session_epoch

	writeCompactArrayLen(buf, 1)
	writeUUID(buf, topicID)

	writeCompactArrayLen(buf, 1)
	writeInt32(buf, partition)
	writeInt32(buf, -1)
	writeInt64(buf, 0)
	writeInt32(buf, -1)
	writeInt64(buf, 0)
	writeInt32(buf, 1048576)
	writeUVarInt(buf, 0) // partition tagged fields

	writeUVarInt(buf, 0) // topic tagged fields

	writeCompactArrayLen(buf, 0) // forgotten topics
	writeCompactString(buf, "")
	writeUVarInt(buf, 0) // request tagged fields

	return buf.Bytes()
}

func verifyFetchResponse(payload []byte, expectedCorrelation int32, topicID [16]byte, partition int32) error {
	reader := bytes.NewReader(payload)

	correlation, err := readInt32(reader)
	if err != nil {
		return fmt.Errorf("read correlation id: %w", err)
	}
	if correlation != expectedCorrelation {
		return fmt.Errorf("correlation id mismatch: got %d want %d", correlation, expectedCorrelation)
	}

	throttle, err := readInt32(reader)
	if err != nil {
		return fmt.Errorf("read throttle: %w", err)
	}
	if throttle != 0 {
		return fmt.Errorf("throttle_time_ms=%d want 0", throttle)
	}

	errorCode, err := readInt16(reader)
	if err != nil {
		return fmt.Errorf("read top-level error: %w", err)
	}
	if errorCode != 0 {
		return fmt.Errorf("fetch error code=%d want 0", errorCode)
	}

	sessionID, err := readInt32(reader)
	if err != nil {
		return fmt.Errorf("read session id: %w", err)
	}
	if sessionID != 0 {
		return fmt.Errorf("session id=%d want 0", sessionID)
	}

	topicCount, err := readCompactArrayLen(reader)
	if err != nil {
		return fmt.Errorf("read response topics: %w", err)
	}
	if topicCount != 1 {
		return fmt.Errorf("topic count=%d want 1", topicCount)
	}

	gotTopicID, err := readUUID(reader)
	if err != nil {
		return fmt.Errorf("read topic id: %w", err)
	}
	if gotTopicID != topicID {
		return fmt.Errorf("topic id mismatch: got %s want %s", hex.EncodeToString(gotTopicID[:]), hex.EncodeToString(topicID[:]))
	}

	partitionCount, err := readCompactArrayLen(reader)
	if err != nil {
		return fmt.Errorf("read partitions len: %w", err)
	}
	if partitionCount != 1 {
		return fmt.Errorf("partition count=%d want 1", partitionCount)
	}

	partIndex, err := readInt32(reader)
	if err != nil {
		return fmt.Errorf("read partition index: %w", err)
	}
	if partIndex != partition {
		return fmt.Errorf("partition index=%d want %d", partIndex, partition)
	}

	partErr, err := readInt16(reader)
	if err != nil {
		return fmt.Errorf("read partition error: %w", err)
	}
	if partErr != 0 {
		return fmt.Errorf("partition error=%d want 0", partErr)
	}

	highWatermark, err := readInt64(reader)
	if err != nil {
		return fmt.Errorf("read high watermark: %w", err)
	}
	if highWatermark != 0 {
		return fmt.Errorf("high watermark=%d want 0", highWatermark)
	}

	lastStable, err := readInt64(reader)
	if err != nil {
		return fmt.Errorf("read last stable offset: %w", err)
	}
	if lastStable != 0 {
		return fmt.Errorf("last stable offset=%d want 0", lastStable)
	}

	logStart, err := readInt64(reader)
	if err != nil {
		return fmt.Errorf("read log start offset: %w", err)
	}
	if logStart != 0 {
		return fmt.Errorf("log start offset=%d want 0", logStart)
	}

	abortedCount, err := readCompactArrayLen(reader)
	if err != nil {
		return fmt.Errorf("read aborted transactions: %w", err)
	}
	if abortedCount != 0 {
		return fmt.Errorf("expected no aborted transactions, got %d", abortedCount)
	}

	preferredReplica, err := readInt32(reader)
	if err != nil {
		return fmt.Errorf("read preferred replica: %w", err)
	}
	if preferredReplica != -1 {
		return fmt.Errorf("preferred replica=%d want -1", preferredReplica)
	}

	records, err := readCompactNullableBytes(reader)
	if err != nil {
		return fmt.Errorf("read records: %w", err)
	}
	if len(records) != 0 {
		return fmt.Errorf("expected empty records, got %d bytes", len(records))
	}

	if err := skipTaggedFields(reader); err != nil {
		return fmt.Errorf("partition tagged fields: %w", err)
	}
	if err := skipTaggedFields(reader); err != nil {
		return fmt.Errorf("topic tagged fields: %w", err)
	}
	if err := skipTaggedFields(reader); err != nil {
		return fmt.Errorf("response tagged fields: %w", err)
	}

	if reader.Len() != 0 {
		return fmt.Errorf("response has %d trailing bytes", reader.Len())
	}

	fmt.Println("Fetch response validation passed")
	return nil
}

func parseUUID(raw string) ([16]byte, error) {
	var out [16]byte
	cleaned := strings.ReplaceAll(strings.TrimSpace(raw), "-", "")
	if len(cleaned) != 32 {
		return out, fmt.Errorf("expected 32 hex characters, got %d", len(cleaned))
	}
	bytes, err := hex.DecodeString(cleaned)
	if err != nil {
		return out, err
	}
	copy(out[:], bytes)
	return out, nil
}

func writeInt16(buf *bytes.Buffer, v int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	buf.Write(tmp[:])
}

func writeInt32(buf *bytes.Buffer, v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	buf.Write(tmp[:])
}

func writeInt64(buf *bytes.Buffer, v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	buf.Write(tmp[:])
}

func writeCompactArrayLen(buf *bytes.Buffer, n int) {
	writeUVarInt(buf, uint64(n+1))
}

func writeCompactNullableString(buf *bytes.Buffer, value *string) {
	if value == nil {
		writeUVarInt(buf, 0)
		return
	}
	writeUVarInt(buf, uint64(len(*value)+1))
	buf.WriteString(*value)
}

func writeCompactString(buf *bytes.Buffer, value string) {
	writeUVarInt(buf, uint64(len(value)+1))
	if len(value) > 0 {
		buf.WriteString(value)
	}
}

func writeUUID(buf *bytes.Buffer, id [16]byte) {
	buf.Write(id[:])
}

func writeUVarInt(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func readInt16(r *bytes.Reader) (int16, error) {
	var tmp [2]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(tmp[:])), nil
}

func readInt32(r *bytes.Reader) (int32, error) {
	var tmp [4]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp[:])), nil
}

func readInt64(r *bytes.Reader) (int64, error) {
	var tmp [8]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(tmp[:])), nil
}

func readCompactArrayLen(r *bytes.Reader) (int, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	return int(n - 1), nil
}

func readUUID(r *bytes.Reader) ([16]byte, error) {
	var id [16]byte
	if _, err := io.ReadFull(r, id[:]); err != nil {
		return id, err
	}
	return id, nil
}

func readCompactNullableBytes(r *bytes.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	size := int(n - 1)
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func skipTaggedFields(r *bytes.Reader) error {
	countPlusOne, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if countPlusOne == 0 {
		return nil
	}
	count := int(countPlusOne - 1)
	for i := 0; i < count; i++ {
		if _, err := binary.ReadUvarint(r); err != nil {
			return err
		}
		sizePlusOne, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		if sizePlusOne == 0 {
			return errors.New("tagged field size zero")
		}
		size := int(sizePlusOne - 1)
		if _, err := io.CopyN(io.Discard, r, int64(size)); err != nil {
			return err
		}
	}
	return nil
}
