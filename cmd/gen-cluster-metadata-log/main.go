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
)

type partitionSpec struct {
	id             int32
	leaderID       int32
	replicas       []int32
	leaderEpoch    int32
	partitionEpoch int32
}

func main() {
	var (
		outputPath     = flag.String("output", "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", "metadata log output path")
		topicName      = flag.String("topic", "", "topic name (required)")
		topicIDRaw     = flag.String("topic-id", "00000000-0000-0000-0000-000000000001", "topic UUID (32 hex or UUID format)")
		replicasRaw    = flag.String("replicas", "1,2", "comma-separated broker IDs used for every partition")
		leadersRaw     = flag.String("leaders", "", "comma-separated leader IDs per partition (optional)")
		partitionCount = flag.Int("partition-count", 2, "number of partitions to generate")
		leaderEpoch    = flag.Int("leader-epoch-start", 0, "base leader epoch; incremented per partition")
	)
	flag.Parse()

	if *topicName == "" {
		fmt.Fprintln(os.Stderr, "missing -topic")
		os.Exit(2)
	}
	if *partitionCount <= 0 {
		fmt.Fprintln(os.Stderr, "-partition-count must be positive")
		os.Exit(2)
	}

	topicID, err := parseUUID(*topicIDRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -topic-id: %v\n", err)
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

	partitions := make([]partitionSpec, 0, *partitionCount)
	for i := 0; i < *partitionCount; i++ {
		leaderID := replicas[0]
		if len(leaders) > i {
			leaderID = leaders[i]
		}
		partitions = append(partitions, partitionSpec{
			id:             int32(i),
			leaderID:       leaderID,
			replicas:       replicas,
			leaderEpoch:    int32(*leaderEpoch + i),
			partitionEpoch: 0,
		})
	}

	payload, err := buildLog(*topicName, topicID, partitions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build metadata log: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(*outputPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outputPath, payload, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write log: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Wrote metadata log for topic %q (%s) with %d partitions to %s\n", *topicName, formatUUID(topicID), len(partitions), *outputPath)
}

func parseUUID(raw string) ([16]byte, error) {
	var id [16]byte
	clean := strings.ReplaceAll(strings.TrimSpace(raw), "-", "")
	if len(clean) != 32 {
		return id, fmt.Errorf("expected 32 hex characters, got %d", len(clean))
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

func buildLog(topicName string, topicID [16]byte, partitions []partitionSpec) ([]byte, error) {
	var records [][]byte

	topicRecord := buildTopicRecord(topicName, topicID)
	records = append(records, topicRecord)

	for i, part := range partitions {
		record, err := buildPartitionRecord(int32(i), topicID, part)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	batch := buildRecordBatch(records)
	buffer := bytes.NewBuffer(make([]byte, 0, len(batch)+12))
	writeInt64(buffer, 0)
	writeInt32(buffer, int32(len(batch)))
	buffer.Write(batch)
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
	if len(spec.replicas) == 0 {
		return nil, fmt.Errorf("partition %d missing replicas", spec.id)
	}

	body := bytes.NewBuffer(make([]byte, 0, 128))
	writeUVarInt(body, 1) // frame version
	writeUVarInt(body, 3) // api key PartitionRecord
	writeUVarInt(body, 0) // version

	writeInt32(body, spec.id)
	writeUUID(body, topicID)
	writeCompactInt32Array(body, spec.replicas)
	writeCompactInt32Array(body, spec.replicas) // ISR mirrors replicas
	writeCompactInt32Array(body, []int32{})     // removing replicas
	writeCompactInt32Array(body, []int32{})     // adding replicas
	writeInt32(body, spec.leaderID)
	writeInt32(body, spec.leaderEpoch)
	writeInt32(body, spec.partitionEpoch)
	writeCompactInt32Array(body, []int32{}) // offline replicas
	writeTaggedFieldMap(body, nil)

	return wrapRecord(body.Bytes(), offsetDelta), nil
}

func wrapRecord(value []byte, offsetDelta int32) []byte {
	rec := bytes.NewBuffer(make([]byte, 0, len(value)+16))
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

func writeCompactString(buf *bytes.Buffer, value string) {
	writeUVarInt(buf, uint64(len(value)+1))
	buf.WriteString(value)
}

func writeCompactInt32Array(buf *bytes.Buffer, values []int32) {
	writeUVarInt(buf, uint64(len(values)+1))
	for _, v := range values {
		writeInt32(buf, v)
	}
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

type taggedField struct {
	Tag   uint32
	Value []byte
}

func writeVarInt(buf *bytes.Buffer, v int64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func writeUVarInt(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func writeInt32(buf *bytes.Buffer, v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	buf.Write(tmp[:])
}

func writeInt16(buf *bytes.Buffer, v int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	buf.Write(tmp[:])
}

func writeInt64(buf *bytes.Buffer, v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	buf.Write(tmp[:])
}

func writeUUID(buf *bytes.Buffer, id [16]byte) {
	buf.Write(id[:])
}

func formatUUID(id [16]byte) string {
	hexed := hex.EncodeToString(id[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexed[0:8], hexed[8:12], hexed[12:16], hexed[16:20], hexed[20:])
}
