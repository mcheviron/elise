package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mcheviron/elise/internal/metadata"
)

type topicInputList []string

func (t *topicInputList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("empty topic value")
	}
	*t = append(*t, value)
	return nil
}

func (t *topicInputList) String() string {
	return strings.Join(*t, ",")
}

type topicSpec struct {
	Name    string
	TopicID [16]byte
}

type partitionSpec struct {
	ID             int32
	LeaderID       int32
	Replicas       []int32
	LeaderEpoch    int32
	PartitionEpoch int32
}

type taggedField struct {
	Tag   uint32
	Value []byte
}

func main() {
	var (
		outputPath     = flag.String("output", metadata.DefaultLogPath, "metadata log output path")
		replicasRaw    = flag.String("replicas", "1,2", "comma-separated broker IDs used for every partition")
		leadersRaw     = flag.String("leaders", "", "comma-separated leader IDs per partition (optional, overrides replicas[0])")
		partitionCount = flag.Int("partition-count", 2, "number of partitions per topic")
		leaderEpoch    = flag.Int("leader-epoch-start", 0, "base leader epoch; incremented per partition and topic")
	)

	var topicInputs topicInputList
	flag.Var(&topicInputs, "topic", "topic spec in the form name[:uuid]; repeat for multiple topics")
	flag.Parse()

	if len(topicInputs) == 0 {
		fmt.Fprintln(os.Stderr, "at least one -topic is required")
		os.Exit(2)
	}
	if *partitionCount <= 0 {
		fmt.Fprintln(os.Stderr, "-partition-count must be positive")
		os.Exit(2)
	}

	replicas, err := parseIntList(*replicasRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -replicas: %v\n", err)
		os.Exit(2)
	}
	if len(replicas) == 0 {
		fmt.Fprintln(os.Stderr, "-replicas must specify at least one broker id")
		os.Exit(2)
	}

	leaders, err := parseOptionalIntList(*leadersRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -leaders: %v\n", err)
		os.Exit(2)
	}
	if len(leaders) > 0 && len(leaders) != *partitionCount {
		fmt.Fprintf(os.Stderr, "-leaders must have %d entries when provided\n", *partitionCount)
		os.Exit(2)
	}

	topics := make([]topicSpec, 0, len(topicInputs))
	for idx, raw := range topicInputs {
		spec, err := parseTopicSpec(raw, idx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -topic %q: %v\n", raw, err)
			os.Exit(2)
		}
		topics = append(topics, spec)
	}

	partitionsByTopic := make(map[string][]partitionSpec, len(topics))
	epochBase := int32(*leaderEpoch)
	for topicIdx, topic := range topics {
		partitions := make([]partitionSpec, 0, *partitionCount)
		for i := 0; i < *partitionCount; i++ {
			leaderID := replicas[0]
			if len(leaders) > 0 {
				leaderID = leaders[i]
			}
			partitions = append(partitions, partitionSpec{
				ID:             int32(i),
				LeaderID:       leaderID,
				Replicas:       replicas,
				LeaderEpoch:    epochBase + int32(topicIdx*(*partitionCount)+i),
				PartitionEpoch: 0,
			})
		}
		partitionsByTopic[topic.Name] = partitions
	}

	payload, err := buildLog(topics, partitionsByTopic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build metadata log: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(*outputPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}
	tmpPath := *outputPath + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write temp log: %v\n", err)
		os.Exit(1)
	}
	if err := os.Rename(tmpPath, *outputPath); err != nil {
		fmt.Fprintf(os.Stderr, "rename temp log: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Wrote metadata log with %d topics to %s\n", len(topics), *outputPath)
	for _, topic := range topics {
		fmt.Printf("  - %s (%s)\n", topic.Name, formatUUID(topic.TopicID))
	}
}

func parseTopicSpec(raw string, idx int) (topicSpec, error) {
	parts := strings.SplitN(raw, ":", 2)
	name := strings.TrimSpace(parts[0])
	if name == "" {
		return topicSpec{}, fmt.Errorf("topic name must not be empty")
	}
	if strings.Contains(name, "\x00") {
		return topicSpec{}, fmt.Errorf("topic name may not contain NUL bytes")
	}

	var id [16]byte
	if len(parts) == 2 {
		var err error
		id, err = parseUUID(parts[1])
		if err != nil {
			return topicSpec{}, err
		}
	} else {
		id = autoTopicID(idx)
	}
	return topicSpec{Name: name, TopicID: id}, nil
}

func autoTopicID(idx int) [16]byte {
	var id [16]byte
	copy(id[:], []byte("auto-topic------"))
	binary.BigEndian.PutUint32(id[12:], uint32(idx+1))
	return id
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

func parseIntList(raw string) ([]int32, error) {
	items := strings.Split(raw, ",")
	out := make([]int32, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		value, err := strconv.ParseInt(item, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", item, err)
		}
		out = append(out, int32(value))
	}
	return out, nil
}

func parseOptionalIntList(raw string) ([]int32, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	return parseIntList(raw)
}

func buildLog(topics []topicSpec, partitions map[string][]partitionSpec) ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, 512))
	baseOffset := int64(0)

	for _, topic := range topics {
		recordBuf := bytes.NewBuffer(nil)

		topicRecord := buildTopicRecord(topic.Name, topic.TopicID)
		records := [][]byte{topicRecord}

		offsetDelta := int32(1)
		for _, part := range partitions[topic.Name] {
			record, err := buildPartitionRecord(offsetDelta, topic.TopicID, part)
			if err != nil {
				return nil, err
			}
			records = append(records, record)
			offsetDelta++
		}

		batch := buildRecordBatch(records)
		writeInt64(recordBuf, baseOffset)
		writeInt32(recordBuf, int32(len(batch)))
		recordBuf.Write(batch)

		buffer.Write(recordBuf.Bytes())
		baseOffset++
	}

	return buffer.Bytes(), nil
}

func buildTopicRecord(name string, id [16]byte) []byte {
	body := bytes.NewBuffer(make([]byte, 0, 64))
	writeUVarInt(body, 1) // frame version
	writeUVarInt(body, 2) // api key TopicRecord
	writeUVarInt(body, 0) // version
	writeCompactString(body, name)
	writeUUID(body, id)
	writeTaggedFields(body, nil)
	return wrapRecord(body.Bytes(), 0)
}

func buildPartitionRecord(offsetDelta int32, topicID [16]byte, spec partitionSpec) ([]byte, error) {
	if len(spec.Replicas) == 0 {
		return nil, fmt.Errorf("partition %d missing replicas", spec.ID)
	}

	body := bytes.NewBuffer(make([]byte, 0, 128))
	writeUVarInt(body, 1) // frame version
	writeUVarInt(body, 3) // api key PartitionRecord
	writeUVarInt(body, 0) // version

	writeInt32(body, spec.ID)
	writeUUID(body, topicID)
	writeCompactInt32Array(body, spec.Replicas)
	writeCompactInt32Array(body, spec.Replicas) // ISR mirrors replicas
	writeCompactInt32Array(body, []int32{})     // removing replicas
	writeCompactInt32Array(body, []int32{})     // adding replicas
	writeInt32(body, spec.LeaderID)
	writeInt32(body, spec.LeaderEpoch)
	writeInt32(body, spec.PartitionEpoch)
	writeCompactInt32Array(body, []int32{}) // offline replicas
	writeTaggedFieldMap(body, nil)

	return wrapRecord(body.Bytes(), offsetDelta), nil
}

func wrapRecord(value []byte, offsetDelta int32) []byte {
	rec := bytes.NewBuffer(make([]byte, 0, len(value)+32))
	rec.WriteByte(0)    // attributes
	writeVarInt(rec, 0) // timestamp delta
	writeVarInt(rec, int64(offsetDelta))
	writeVarInt(rec, 0) // key length (empty)
	writeVarInt(rec, int64(len(value)))
	rec.Write(value)
	writeVarInt(rec, 0) // headers count

	out := bytes.NewBuffer(make([]byte, 0, rec.Len()+binary.MaxVarintLen64))
	writeVarInt(out, int64(rec.Len()))
	out.Write(rec.Bytes())
	return out.Bytes()
}

func buildRecordBatch(records [][]byte) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 256))
	writeInt32(buf, 0) // partition leader epoch
	buf.WriteByte(2)   // magic
	writeInt32(buf, 0) // crc placeholder
	writeInt16(buf, 0) // attributes
	writeInt32(buf, int32(len(records)-1))
	writeInt64(buf, 0) // base timestamp
	writeInt64(buf, 0) // max timestamp
	writeInt64(buf, 0) // producer id
	writeInt16(buf, 0) // producer epoch
	writeInt32(buf, 0) // base sequence
	writeInt32(buf, int32(len(records)))
	for _, rec := range records {
		buf.Write(rec)
	}
	return buf.Bytes()
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

func writeCompactInt32Array(buf *bytes.Buffer, values []int32) {
	writeUVarInt(buf, uint64(len(values)+1))
	for _, v := range values {
		writeInt32(buf, v)
	}
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

func writeTaggedFields(buf *bytes.Buffer, fields []taggedField) {
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

func writeTaggedFieldMap(buf *bytes.Buffer, fields map[uint32][]byte) {
	if len(fields) == 0 {
		writeUVarInt(buf, 0)
		return
	}
	writeUVarInt(buf, uint64(len(fields)+1))
	for tag, value := range fields {
		writeUVarInt(buf, uint64(tag))
		writeUVarInt(buf, uint64(len(value)+1))
		buf.Write(value)
	}
}

func writeUVarInt(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func writeVarInt(buf *bytes.Buffer, v int64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func formatUUID(id [16]byte) string {
	b := make([]byte, 16)
	copy(b, id[:])
	raw := hex.EncodeToString(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", raw[0:8], raw[8:12], raw[12:16], raw[16:20], raw[20:])
}
