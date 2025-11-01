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
	"sort"
	"strings"
	"time"

	"github.com/mcheviron/elise/internal/protocol"
)

type topicList []string

func (t *topicList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("empty topic value")
	}
	*t = append(*t, value)
	return nil
}

func (t *topicList) String() string {
	return strings.Join(*t, ",")
}

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:9092", "broker address")
		timeout     = flag.Duration("timeout", 5*time.Second, "network timeout")
		correlation = flag.Int("correlation", 42, "correlation id")
	)

	var topics topicList
	flag.Var(&topics, "topic", "topic name to include (repeat; at least two)")
	flag.Parse()

	if len(topics) < 2 {
		fmt.Fprintln(os.Stderr, "provide at least two -topic values")
		os.Exit(2)
	}

	if err := describeTopics(*addr, *timeout, int32(*correlation), topics); err != nil {
		fmt.Fprintf(os.Stderr, "describe-topic-partitions-multi: %v\n", err)
		os.Exit(1)
	}
}

func describeTopics(addr string, timeout time.Duration, correlationID int32, topics []string) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	frame, err := buildRequestFrame(correlationID, topics)
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
	responseLen := binary.BigEndian.Uint32(sizePrefix[:])
	payload := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return fmt.Errorf("read response payload: %w", err)
	}

	fmt.Printf("Response length prefix: %d bytes\n", responseLen)

	reader := bytes.NewReader(payload)

	respCorrelation, err := readInt32(reader)
	if err != nil {
		return fmt.Errorf("read correlation id: %w", err)
	}
	fmt.Printf("Correlation ID: %d\n", respCorrelation)

	throttle, err := readInt32(reader)
	if err != nil {
		return fmt.Errorf("read throttle time: %w", err)
	}
	fmt.Printf("ThrottleTimeMs: %d\n", throttle)

	topicsResp, err := parseTopics(reader)
	if err != nil {
		return fmt.Errorf("parse topics: %w", err)
	}

	if err := skipTaggedFields(reader); err != nil {
		return fmt.Errorf("skip response tagged fields: %w", err)
	}

	printTopics(topicsResp)

	order := make([]string, len(topicsResp))
	for i, t := range topicsResp {
		order[i] = t.Name
	}
	sorted := append([]string(nil), order...)
	sort.Strings(sorted)
	if equalStringSlices(order, sorted) {
		fmt.Printf("Topic order OK: %v\n", order)
	} else {
		fmt.Printf("Topic order mismatch: got %v want %v\n", order, sorted)
	}

	return nil
}

func buildRequestFrame(correlationID int32, topics []string) ([]byte, error) {
	body := buildRequestBody(topics)

	header := bytes.NewBuffer(make([]byte, 0, 32))
	writeInt16(header, int16(protocol.APIKeyDescribeTopicPartitions))
	writeInt16(header, 0)             // api version
	writeInt32(header, correlationID) // correlation id
	writeCompactNullableString(header, nil)
	writeTaggedFields(header, nil)

	out := bytes.NewBuffer(make([]byte, 0, header.Len()+len(body)+4))
	writeInt32(out, int32(header.Len()+len(body)))
	out.Write(header.Bytes())
	out.Write(body)
	return out.Bytes(), nil
}

func buildRequestBody(topics []string) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	writeCompactArrayLen(buf, len(topics))
	for _, name := range topics {
		name := name // create copy for pointer capture
		writeCompactNullableString(buf, &name)
		writeUUID(buf, [16]byte{})
		writeCompactArrayLen(buf, 0) // no partition filters
		writeTaggedFields(buf, nil)
	}
	writeInt32(buf, -1) // response_partition_limit
	writeCompactNullableCursor(buf, nil)
	writeTaggedFields(buf, nil)
	return buf.Bytes()
}

type topicResponse struct {
	Name       string
	ErrorCode  int16
	TopicID    [16]byte
	IsInternal bool
	Partitions []partitionResponse
}

type partitionResponse struct {
	PartitionIndex int32
	ErrorCode      int16
	LeaderID       int32
	LeaderEpoch    int32
	Replicas       []int32
	ISR            []int32
	Offline        []int32
}

func parseTopics(r *bytes.Reader) ([]topicResponse, error) {
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
			return nil, fmt.Errorf("topic[%d] is_internal: %w", i, err)
		}
		partitions, err := parsePartitions(r, i)
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
			Name:       name,
			ErrorCode:  int16(errCode),
			TopicID:    topicID,
			IsInternal: internal,
			Partitions: partitions,
		})
	}

	return out, nil
}

func parsePartitions(r *bytes.Reader, topicIndex int) ([]partitionResponse, error) {
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
			return nil, fmt.Errorf("topic[%d] partition[%d] offline: %w", topicIndex, j, err)
		}
		if err := skipTaggedFields(r); err != nil {
			return nil, fmt.Errorf("topic[%d] partition[%d] tagged fields: %w", topicIndex, j, err)
		}
		out = append(out, partitionResponse{
			PartitionIndex: index,
			ErrorCode:      int16(errCode),
			LeaderID:       leader,
			LeaderEpoch:    leaderEpoch,
			Replicas:       replicas,
			ISR:            isr,
			Offline:        offline,
		})
	}
	return out, nil
}

func printTopics(topics []topicResponse) {
	fmt.Println("Topics returned:")
	for _, topic := range topics {
		fmt.Printf("  %s error=%d internal=%v\n", topic.Name, topic.ErrorCode, topic.IsInternal)
		fmt.Printf("    ID: %s\n", hex.EncodeToString(topic.TopicID[:]))
		fmt.Printf("    Partitions (%d):\n", len(topic.Partitions))
		for _, part := range topic.Partitions {
			fmt.Printf("      [%d] error=%d leader=%d epoch=%d replicas=%v isr=%v offline=%v\n",
				part.PartitionIndex,
				part.ErrorCode,
				part.LeaderID,
				part.LeaderEpoch,
				part.Replicas,
				part.ISR,
				part.Offline,
			)
		}
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func writeCompactArrayLen(buf *bytes.Buffer, length int) {
	writeUVarInt(buf, uint64(length+1))
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

func writeUVarInt(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}
