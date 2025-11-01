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
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mcheviron/elise/internal/metadata"
)

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:9092", "broker address")
		metadataLog = flag.String("metadata", metadata.DefaultLogPath, "metadata log path")
		correlation = flag.Int("correlation", 501, "correlation id to use")
		timeout     = flag.Duration("timeout", 5*time.Second, "network timeout")
	)
	flag.Parse()

	cluster, err := metadata.Load(*metadataLog)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load metadata: %v\n", err)
		os.Exit(1)
	}

	logRoot := filepath.Dir(filepath.Dir(*metadataLog))
	selection, err := selectPartition(cluster, logRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to select topic partition: %v\n", err)
		os.Exit(1)
	}

	expectedRecords, baseOffset, lastOffset, err := readLatestRecordBatch(selection.segmentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read record batch: %v\n", err)
		os.Exit(1)
	}

	response, err := fetchPartition(*addr, selection.topic.ID, selection.partition.PartitionID, int32(*correlation), *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch request failed: %v\n", err)
		os.Exit(1)
	}

	if err := verifyFetchResponse(response, int32(*correlation), selection.topic.ID, selection.partition.PartitionID, baseOffset, lastOffset+1, expectedRecords); err != nil {
		fmt.Fprintf(os.Stderr, "fetch verification failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Fetch verification succeeded for topic %s (%s) partition %d\n", selection.topic.Name, formatUUID(selection.topic.ID), selection.partition.PartitionID)
}

type selectedPartition struct {
	topic       *metadata.Topic
	partition   *metadata.Partition
	segmentPath string
}

func selectPartition(cluster *metadata.Cluster, logRoot string) (*selectedPartition, error) {
	topics := cluster.Topics()
	if len(topics) == 0 {
		return nil, errors.New("metadata contained no topics")
	}

	for _, topic := range topics {
		if strings.TrimSpace(topic.Name) == "" {
			continue
		}
		for id, partition := range topic.Partitions {
			segmentDir := filepath.Join(logRoot, fmt.Sprintf("%s-%d", topic.Name, id))
			segment, err := latestLogSegment(segmentDir)
			if err != nil {
				continue
			}
			return &selectedPartition{
				topic:       topic,
				partition:   partition,
				segmentPath: segment,
			}, nil
		}
	}

	return nil, fmt.Errorf("no topic partitions with log segments found under %s", logRoot)
}

func latestLogSegment(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
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
		return "", fmt.Errorf("no log segments in %s", dir)
	}

	sort.Strings(segments)
	return filepath.Join(dir, segments[len(segments)-1]), nil
}

func readLatestRecordBatch(path string) ([]byte, int64, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, err
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
		return 0, fmt.Errorf("read magic: %w", err)
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
	if attributes&0x7 != 0 {
		return 0, fmt.Errorf("compressed batches not supported (type %d)", attributes&0x7)
	}

	var lastOffsetDelta int32
	if err := binary.Read(reader, binary.BigEndian, &lastOffsetDelta); err != nil {
		return 0, fmt.Errorf("read last offset delta: %w", err)
	}

	// Skip remaining header fields we do not need.
	if err := skipN(reader, 8*3); err != nil {
		return 0, err
	}
	if err := skipN(reader, 2); err != nil {
		return 0, err
	}
	if err := skipN(reader, 4); err != nil {
		return 0, err
	}

	var recordCount int32
	if err := binary.Read(reader, binary.BigEndian, &recordCount); err != nil {
		return 0, fmt.Errorf("read record count: %w", err)
	}
	if recordCount != 1 {
		return 0, fmt.Errorf("expected 1 record, got %d", recordCount)
	}

	_ = partitionLeaderEpoch
	_ = crc
	return baseOffset + int64(lastOffsetDelta), nil
}

func skipN(r *bytes.Reader, n int64) error {
	if _, err := r.Seek(n, io.SeekCurrent); err != nil {
		return err
	}
	return nil
}

func fetchPartition(addr string, topicID [16]byte, partitionID int32, correlationID int32, timeout time.Duration) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set deadline: %w", err)
	}

	frame := buildFetchRequestFrame(topicID, partitionID, correlationID)
	if _, err := conn.Write(frame); err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	var sizePrefix [4]byte
	if _, err := io.ReadFull(conn, sizePrefix[:]); err != nil {
		return nil, fmt.Errorf("read response size: %w", err)
	}
	messageSize := int(binary.BigEndian.Uint32(sizePrefix[:]))
	if messageSize <= 0 {
		return nil, fmt.Errorf("invalid response size %d", messageSize)
	}

	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, fmt.Errorf("read response payload: %w", err)
	}

	return payload, nil
}

func buildFetchRequestFrame(topicID [16]byte, partition int32, correlationID int32) []byte {
	body := buildFetchRequestBody(topicID, partition)

	header := bytes.NewBuffer(make([]byte, 0, 32))
	writeInt16(header, 1)  // API Key Fetch
	writeInt16(header, 16) // version
	writeInt32(header, correlationID)
	writeCompactNullableString(header, nil)
	writeUVarInt(header, 0)

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
	writeInt32(buf, 0)  // current_leader_epoch
	writeInt64(buf, 0)  // fetch_offset
	writeInt32(buf, -1) // log_start_offset
	writeInt64(buf, 0)  // partition_max_bytes placeholder
	writeInt32(buf, 1048576)
	writeUVarInt(buf, 0)

	writeUVarInt(buf, 0) // topic tagged fields

	writeCompactArrayLen(buf, 0) // forgotten topics
	writeCompactString(buf, "")
	writeUVarInt(buf, 0) // request tagged fields

	return buf.Bytes()
}

func verifyFetchResponse(payload []byte, expectedCorrelation int32, topicID [16]byte, partition int32, logStart int64, highWater int64, expectedRecords []byte) error {
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
		return fmt.Errorf("unexpected throttle_time_ms=%d", throttle)
	}

	topLevelErr, err := readInt16(reader)
	if err != nil {
		return fmt.Errorf("read top level error: %w", err)
	}
	if topLevelErr != 0 {
		return fmt.Errorf("top level error=%d", topLevelErr)
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
		return fmt.Errorf("read partition array: %w", err)
	}
	if partitionCount != 1 {
		return fmt.Errorf("partition count=%d want 1", partitionCount)
	}

	partIdx, err := readInt32(reader)
	if err != nil {
		return fmt.Errorf("read partition index: %w", err)
	}
	if partIdx != partition {
		return fmt.Errorf("partition index=%d want %d", partIdx, partition)
	}

	partErr, err := readInt16(reader)
	if err != nil {
		return fmt.Errorf("read partition error: %w", err)
	}
	if partErr != 0 {
		return fmt.Errorf("partition error=%d", partErr)
	}

	gotHighWater, err := readInt64(reader)
	if err != nil {
		return fmt.Errorf("read high watermark: %w", err)
	}
	if gotHighWater != highWater {
		return fmt.Errorf("high watermark=%d want %d", gotHighWater, highWater)
	}

	lastStable, err := readInt64(reader)
	if err != nil {
		return fmt.Errorf("read last stable offset: %w", err)
	}
	if lastStable != highWater {
		return fmt.Errorf("last stable offset=%d want %d", lastStable, highWater)
	}

	logStartOffset, err := readInt64(reader)
	if err != nil {
		return fmt.Errorf("read log start offset: %w", err)
	}
	if logStartOffset != logStart {
		return fmt.Errorf("log start offset=%d want %d", logStartOffset, logStart)
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
	if records == nil {
		return fmt.Errorf("records were null")
	}
	if !bytes.Equal(records, expectedRecords) {
		return fmt.Errorf("records payload mismatch: got %d bytes want %d", len(records), len(expectedRecords))
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
		return fmt.Errorf("response had %d trailing bytes", reader.Len())
	}

	return nil
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

func formatUUID(id [16]byte) string {
	raw := hex.EncodeToString(id[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", raw[0:8], raw[8:12], raw[12:16], raw[16:20], raw[20:])
}
