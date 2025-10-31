package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mcheviron/elise/internal/protocol"
)

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:9092", "broker address")
		topicName   = flag.String("topic", "", "topic name to describe")
		topicIDRaw  = flag.String("topic-id", "", "topic UUID (optional)")
		partitions  = flag.String("partitions", "", "comma-separated partition indexes to request")
		timeout     = flag.Duration("timeout", 5*time.Second, "network timeout")
		correlation = flag.Int("correlation", 1, "correlation id")
	)
	flag.Parse()

	if strings.TrimSpace(*topicName) == "" && strings.TrimSpace(*topicIDRaw) == "" {
		fmt.Fprintln(os.Stderr, "set -topic or -topic-id")
		flag.Usage()
		os.Exit(2)
	}

	var (
		topicPtr *string
		topicID  [16]byte
		err      error
	)
	if strings.TrimSpace(*topicName) != "" {
		name := *topicName
		topicPtr = &name
	}
	if strings.TrimSpace(*topicIDRaw) != "" {
		topicID, err = parseUUID(*topicIDRaw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -topic-id: %v\n", err)
			os.Exit(2)
		}
	}
	partitionsList, err := parsePartitions(*partitions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -partitions: %v\n", err)
		os.Exit(2)
	}

	if err := run(*addr, topicPtr, topicID, partitionsList, *timeout, int32(*correlation)); err != nil {
		fmt.Fprintf(os.Stderr, "describe-topic-partitions failed: %v\n", err)
		os.Exit(1)
	}
}

func run(addr string, topicName *string, topicID [16]byte, partitions []int32, timeout time.Duration, correlationID int32) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	frame, err := buildRequestFrame(topicName, topicID, partitions, correlationID)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

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

	printReport(sizePrefix[:], payload)
	return nil
}

func buildRequestFrame(topicName *string, topicID [16]byte, partitions []int32, correlationID int32) ([]byte, error) {
	body := buildRequestBody(topicName, topicID, partitions)

	header := bytes.NewBuffer(make([]byte, 0, 32))
	writeInt16(header, int16(protocol.APIKeyDescribeTopicPartitions))
	writeInt16(header, 0)             // api version
	writeInt32(header, correlationID) // correlation id
	writeCompactNullableString(header, nil)
	writeTaggedFields(header, nil)

	out := bytes.NewBuffer(make([]byte, 0, len(body)+header.Len()+4))
	writeInt32(out, int32(header.Len()+len(body)))
	out.Write(header.Bytes())
	out.Write(body)
	return out.Bytes(), nil
}

func buildRequestBody(topicName *string, topicID [16]byte, partitions []int32) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	writeCompactArrayLen(buf, 1)
	writeCompactNullableString(buf, topicName)
	writeUUID(buf, topicID)
	writePartitionsArray(buf, partitions)
	writeTaggedFields(buf, nil)
	writeInt32(buf, -1) // response_partition_limit = unlimited
	writeCompactNullableCursor(buf, nil)
	writeTaggedFields(buf, nil)
	return buf.Bytes()
}

func printReport(sizePrefix, payload []byte) {
	fmt.Printf("Response size prefix: %s\n", hex.EncodeToString(sizePrefix))

	reader := bytes.NewReader(payload)

	correlationID, err := readInt32(reader)
	if err != nil {
		fmt.Printf("Failed to read correlation id: %v\n", err)
		return
	}
	fmt.Printf("Correlation ID: %d\n", correlationID)

	throttle, err := readInt32(reader)
	if err != nil {
		fmt.Printf("Failed to read throttle time: %v\n", err)
		return
	}

	topics, err := readTopics(reader)
	if err != nil {
		fmt.Printf("Failed to parse topics: %v\n", err)
		return
	}

	if err := skipTaggedFields(reader); err != nil {
		fmt.Printf("Failed to skip response tagged fields: %v\n", err)
		return
	}

	fmt.Printf("ThrottleTimeMs: %d\n", throttle)
	for _, topic := range topics {
		fmt.Printf("Topic %s (error=%d)\n", topic.Name, topic.ErrorCode)
		fmt.Printf("  ID: %s\n", hex.EncodeToString(topic.TopicID[:]))
		fmt.Printf("  Partitions (%d):\n", len(topic.Partitions))
		for _, partition := range topic.Partitions {
			fmt.Printf("    [%d] error=%d leader=%d epoch=%d replicas=%v isr=%v offline=%v\n",
				partition.PartitionIndex,
				partition.ErrorCode,
				partition.LeaderID,
				partition.LeaderEpoch,
				partition.ReplicaNodes,
				partition.IsrNodes,
				partition.OfflineReplicas,
			)
		}
		fmt.Println()
	}
}

type topicResponse struct {
	ErrorCode  ErrorCode
	Name       string
	TopicID    [16]byte
	IsInternal bool
	Partitions []partitionResponse
}

type partitionResponse struct {
	ErrorCode       ErrorCode
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32
	ReplicaNodes    []int32
	IsrNodes        []int32
	OfflineReplicas []int32
}

func readTopics(r *bytes.Reader) ([]topicResponse, error) {
	countPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, err
	}
	if countPlusOne == 0 {
		return nil, nil
	}
	count := int(countPlusOne - 1)
	out := make([]topicResponse, 0, count)
	for i := 0; i < count; i++ {
		errCode, err := readInt16(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] error code: %w", i, err)
		}
		namePtr, err := readCompactNullableString(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] name: %w", i, err)
		}
		name := ""
		if namePtr != nil {
			name = *namePtr
		}
		topicID, err := readUUID(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] id: %w", i, err)
		}
		internal, err := readBool(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] is internal: %w", i, err)
		}
		partitions, err := readPartitions(r, i)
		if err != nil {
			return nil, err
		}
		if err := skipCursor(r); err != nil {
			return nil, fmt.Errorf("topic[%d] cursor: %w", i, err)
		}
		if _, err := readInt32(r); err != nil {
			return nil, fmt.Errorf("topic[%d] authorized ops: %w", i, err)
		}
		if err := skipTaggedFields(r); err != nil {
			return nil, fmt.Errorf("topic[%d] tagged fields: %w", i, err)
		}
		out = append(out, topicResponse{
			ErrorCode:  ErrorCode(errCode),
			Name:       name,
			TopicID:    topicID,
			IsInternal: internal,
			Partitions: partitions,
		})
	}
	return out, nil
}

func readPartitions(r *bytes.Reader, topicIndex int) ([]partitionResponse, error) {
	countPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, fmt.Errorf("topic[%d] partitions length: %w", topicIndex, err)
	}
	if countPlusOne == 0 {
		return nil, nil
	}
	count := int(countPlusOne - 1)
	out := make([]partitionResponse, 0, count)
	for j := 0; j < count; j++ {
		errCode, err := readInt16(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] error code: %w", topicIndex, j, err)
		}
		index, err := readInt32(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] index: %w", topicIndex, j, err)
		}
		leader, err := readInt32(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] leader: %w", topicIndex, j, err)
		}
		leaderEpoch, err := readInt32(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] leader epoch: %w", topicIndex, j, err)
		}
		replicas, err := readCompactInt32Array(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] replicas: %w", topicIndex, j, err)
		}
		isr, err := readCompactInt32Array(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] isr: %w", topicIndex, j, err)
		}
		if err := skipCompactNullableInt32Array(r); err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] eligible leaders: %w", topicIndex, j, err)
		}
		if err := skipCompactNullableInt32Array(r); err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] last known elr: %w", topicIndex, j, err)
		}
		offline, err := readCompactInt32Array(r)
		if err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] offline replicas: %w", topicIndex, j, err)
		}
		if err := skipTaggedFields(r); err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] tagged fields: %w", topicIndex, j, err)
		}
		out = append(out, partitionResponse{
			ErrorCode:       ErrorCode(errCode),
			PartitionIndex:  index,
			LeaderID:        leader,
			LeaderEpoch:     leaderEpoch,
			ReplicaNodes:    replicas,
			IsrNodes:        isr,
			OfflineReplicas: offline,
		})
	}
	return out, nil
}

func parsePartitions(raw string) ([]int32, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int32, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.ParseInt(part, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("partition %q: %w", part, err)
		}
		out = append(out, int32(value))
	}
	return out, nil
}

func parseUUID(raw string) ([16]byte, error) {
	var id [16]byte
	clean := strings.ReplaceAll(strings.TrimSpace(raw), "-", "")
	if len(clean) != 32 {
		return id, fmt.Errorf("uuid must have 32 hex characters")
	}
	bytes, err := hex.DecodeString(clean)
	if err != nil {
		return id, err
	}
	copy(id[:], bytes)
	return id, nil
}

func writePartitionsArray(buf *bytes.Buffer, partitions []int32) {
	if len(partitions) == 0 {
		writeCompactArrayLen(buf, 0)
		return
	}
	writeCompactArrayLen(buf, len(partitions))
	for _, p := range partitions {
		writeInt32(buf, p)
		writeTaggedFields(buf, nil)
	}
}

type ErrorCode int16

func readBool(r *bytes.Reader) (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	return b != 0, nil
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

func readUVarInt(r *bytes.Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func readCompactNullableString(r *bytes.Reader) (*string, error) {
	lengthPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, err
	}
	if lengthPlusOne == 0 {
		return nil, nil
	}
	length := int(lengthPlusOne - 1)
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	str := string(buf)
	return &str, nil
}

func readUUID(r *bytes.Reader) ([16]byte, error) {
	var id [16]byte
	if _, err := io.ReadFull(r, id[:]); err != nil {
		return [16]byte{}, err
	}
	return id, nil
}

func readCompactInt32Array(r *bytes.Reader) ([]int32, error) {
	lengthPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, err
	}
	if lengthPlusOne == 0 {
		return nil, fmt.Errorf("compact array length zero indicates null")
	}
	length := int(lengthPlusOne - 1)
	out := make([]int32, length)
	for i := 0; i < length; i++ {
		val, err := readInt32(r)
		if err != nil {
			return nil, err
		}
		out[i] = val
	}
	return out, nil
}

func skipCompactNullableInt32Array(r *bytes.Reader) error {
	lengthPlusOne, err := readUVarInt(r)
	if err != nil {
		return err
	}
	if lengthPlusOne == 0 {
		return nil
	}
	length := int(lengthPlusOne - 1)
	for i := 0; i < length; i++ {
		if _, err := readInt32(r); err != nil {
			return err
		}
	}
	return nil
}

func skipCursor(r *bytes.Reader) error {
	lengthPlusOne, err := readUVarInt(r)
	if err != nil {
		return err
	}
	if lengthPlusOne == 0 {
		return nil
	}
	length := int(lengthPlusOne - 1)
	if _, err := r.Seek(int64(length), io.SeekCurrent); err != nil {
		return err
	}
	return nil
}

func skipTaggedFields(r *bytes.Reader) error {
	countPlusOne, err := readUVarInt(r)
	if err != nil {
		return err
	}
	if countPlusOne == 0 {
		return nil
	}
	count := int(countPlusOne - 1)
	for i := 0; i < count; i++ {
		if _, err := readUVarInt(r); err != nil {
			return err
		}
		sizePlusOne, err := readUVarInt(r)
		if err != nil {
			return err
		}
		if sizePlusOne == 0 {
			return fmt.Errorf("tagged field length zero")
		}
		size := int(sizePlusOne - 1)
		if _, err := r.Seek(int64(size), io.SeekCurrent); err != nil {
			return err
		}
	}
	return nil
}

func writeCompactArrayLen(buf *bytes.Buffer, length int) {
	writeUVarInt(buf, uint64(length+1))
}

func writeUVarInt(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
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

func writeCompactNullableString(buf *bytes.Buffer, value *string) {
	if value == nil {
		writeUVarInt(buf, 0)
		return
	}
	writeCompactString(buf, *value)
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

func writeTaggedFields(buf *bytes.Buffer, fields []protocol.TaggedField) {
	if len(fields) == 0 {
		writeUVarInt(buf, 0)
		return
	}
	writeUVarInt(buf, uint64(len(fields)+1))
	for _, field := range fields {
		writeUVarInt(buf, uint64(field.Tag))
		writeUVarInt(buf, uint64(len(field.Value)+1))
		buf.Write(field.Value)
	}
}

func writeCompactNullableCursor(buf *bytes.Buffer, cursor interface{}) {
	writeUVarInt(buf, 0)
}
