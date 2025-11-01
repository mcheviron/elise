package metadata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

const DefaultLogPath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

const maxRecordComponentLength = int64(1<<31 - 1)

// Cluster represents the in-memory view built from the metadata log.
type Cluster struct {
	topicsByID   map[[16]byte]*Topic
	topicsByName map[string]*Topic
}

// Topic captures topic-level metadata.
type Topic struct {
	ID         [16]byte
	Name       string
	Partitions map[int32]*Partition
}

// Partition captures partition-level metadata extracted from PartitionRecord entries.
type Partition struct {
	PartitionID              int32
	LeaderID                 int32
	LeaderEpoch              int32
	PartitionEpoch           int32
	Replicas                 []int32
	ISR                      []int32
	RemovingReplicas         []int32
	AddingReplicas           []int32
	EligibleLeaderReplicas   []int32
	LastKnownEligibleLeaders []int32
	OfflineReplicas          []int32
	LeaderRecoveryState      int8
}

// Load reads the metadata log at the provided path and constructs an in-memory view of topics and partitions.
func Load(path string) (*Cluster, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("metadata: open log %q: %w", path, err)
	}
	defer file.Close()

	cluster := newCluster()

	for {
		var baseOffset int64
		if err := binary.Read(file, binary.BigEndian, &baseOffset); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("metadata: read batch base offset: %w", err)
		}

		var batchLength int32
		if err := binary.Read(file, binary.BigEndian, &batchLength); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("metadata: read batch length: %w", err)
		}
		if batchLength < 0 {
			return nil, fmt.Errorf("metadata: negative batch length %d", batchLength)
		}

		batch := make([]byte, batchLength)
		if _, err := io.ReadFull(file, batch); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("metadata: read batch payload: %w", err)
		}

		if err := parseBatch(bytes.NewReader(batch), cluster); err != nil {
			return nil, err
		}
	}

	return cluster, nil
}

// TopicByName returns the topic metadata for the provided name if present.
func (c *Cluster) TopicByName(name string) (*Topic, bool) {
	topic, ok := c.topicsByName[name]
	return topic, ok
}

// TopicByID returns the topic metadata given a topic UUID.
func (c *Cluster) TopicByID(id [16]byte) (*Topic, bool) {
	topic, ok := c.topicsByID[id]
	return topic, ok
}

// Topics returns all topics sorted by name then UUID.
func (c *Cluster) Topics() []*Topic {
	if c == nil {
		return nil
	}
	list := make([]*Topic, 0, len(c.topicsByID))
	for _, t := range c.topicsByID {
		list = append(list, t)
	}
	sort.Slice(list, func(i, j int) bool {
		nameI := list[i].Name
		nameJ := list[j].Name
		if nameI != nameJ {
			return nameI < nameJ
		}
		return bytes.Compare(list[i].ID[:], list[j].ID[:]) < 0
	})
	return list
}

// PartitionList returns partitions sorted by partition id.
func (t *Topic) PartitionList() []*Partition {
	list := make([]*Partition, 0, len(t.Partitions))
	for _, p := range t.Partitions {
		list = append(list, p)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].PartitionID < list[j].PartitionID })
	return list
}

func newCluster() *Cluster {
	return &Cluster{
		topicsByID:   make(map[[16]byte]*Topic),
		topicsByName: make(map[string]*Topic),
	}
}

func parseBatch(r *bytes.Reader, cluster *Cluster) error {
	if _, err := readInt32(r); err != nil {
		return fmt.Errorf("metadata: read partition leader epoch: %w", err)
	}

	magic, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("metadata: read batch magic: %w", err)
	}
	if magic != 2 {
		return fmt.Errorf("metadata: unsupported record batch magic %d", magic)
	}

	if _, err := readInt32(r); err != nil {
		return fmt.Errorf("metadata: read batch crc: %w", err)
	}

	attributes, err := readInt16(r)
	if err != nil {
		return fmt.Errorf("metadata: read batch attributes: %w", err)
	}
	compressionType := attributes & 0x7
	if compressionType != 0 {
		return fmt.Errorf("metadata: compressed batches (type %d) are not supported", compressionType)
	}
	isControlBatch := (attributes & 0x20) != 0

	if _, err := readInt32(r); err != nil {
		return fmt.Errorf("metadata: read last offset delta: %w", err)
	}
	if _, err := readInt64(r); err != nil {
		return fmt.Errorf("metadata: read base timestamp: %w", err)
	}
	if _, err := readInt64(r); err != nil {
		return fmt.Errorf("metadata: read max timestamp: %w", err)
	}
	if _, err := readInt64(r); err != nil {
		return fmt.Errorf("metadata: read producer id: %w", err)
	}
	if _, err := readInt16(r); err != nil {
		return fmt.Errorf("metadata: read producer epoch: %w", err)
	}
	if _, err := readInt32(r); err != nil {
		return fmt.Errorf("metadata: read base sequence: %w", err)
	}

	recordCount, err := readInt32(r)
	if err != nil {
		return fmt.Errorf("metadata: read record count: %w", err)
	}
	if recordCount < 0 {
		return fmt.Errorf("metadata: negative record count %d", recordCount)
	}

	for i := 0; i < int(recordCount); i++ {
		value, err := readRecordValue(r)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			continue
		}
		if isControlBatch {
			continue
		}
		if err := cluster.applyMetadataRecord(value); err != nil {
			return err
		}
	}

	return nil
}

func readRecordValue(r *bytes.Reader) ([]byte, error) {
	length, err := readVarInt(r)
	if err != nil {
		return nil, fmt.Errorf("metadata: read record length: %w", err)
	}
	if length < 0 {
		return nil, fmt.Errorf("metadata: negative record length %d", length)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("metadata: read record payload: %w", err)
	}

	record := bytes.NewReader(payload)

	if _, err := record.ReadByte(); err != nil {
		return nil, fmt.Errorf("metadata: read record attributes: %w", err)
	}
	if _, err := readVarLong(record); err != nil {
		return nil, fmt.Errorf("metadata: read timestamp delta: %w", err)
	}
	if _, err := readVarInt(record); err != nil {
		return nil, fmt.Errorf("metadata: read offset delta: %w", err)
	}

	keyLen, err := readVarInt(record)
	if err != nil {
		return nil, fmt.Errorf("metadata: read key length: %w", err)
	}
	if keyLen > maxRecordComponentLength {
		return nil, fmt.Errorf("metadata: key length too large %d", keyLen)
	}
	if keyLen > 0 {
		if err := skipN(record, int64(keyLen)); err != nil {
			return nil, fmt.Errorf("metadata: skip key: %w", err)
		}
	}

	valueLen, err := readVarInt(record)
	if err != nil {
		return nil, fmt.Errorf("metadata: read value length: %w", err)
	}
	if valueLen < 0 {
		return nil, nil
	}
	if valueLen > maxRecordComponentLength {
		return nil, fmt.Errorf("metadata: value length too large %d", valueLen)
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(record, value); err != nil {
		return nil, fmt.Errorf("metadata: read value: %w", err)
	}

	headerCount, err := readVarInt(record)
	if err != nil {
		return nil, fmt.Errorf("metadata: read headers count: %w", err)
	}
	if headerCount < 0 {
		headerCount = 0
	}
	for i := 0; i < int(headerCount); i++ {
		headerKeyLen, err := readVarInt(record)
		if err != nil {
			return nil, fmt.Errorf("metadata: read header key length: %w", err)
		}
		if headerKeyLen < 0 {
			return nil, fmt.Errorf("metadata: invalid header key length %d", headerKeyLen)
		}
		if err := skipN(record, int64(headerKeyLen)); err != nil {
			return nil, fmt.Errorf("metadata: skip header key: %w", err)
		}
		headerValueLen, err := readVarInt(record)
		if err != nil {
			return nil, fmt.Errorf("metadata: read header value length: %w", err)
		}
		if headerValueLen >= 0 {
			if err := skipN(record, int64(headerValueLen)); err != nil {
				return nil, fmt.Errorf("metadata: skip header value: %w", err)
			}
		}
	}

	return value, nil
}

func (c *Cluster) applyMetadataRecord(value []byte) error {
	reader := bytes.NewReader(value)

	frameVersion, err := readUVarInt(reader)
	if err != nil {
		return fmt.Errorf("metadata: read frame version: %w", err)
	}
	if frameVersion != 1 {
		return fmt.Errorf("metadata: unsupported frame version %d", frameVersion)
	}

	apiKey, err := readUVarInt(reader)
	if err != nil {
		return fmt.Errorf("metadata: read metadata api key: %w", err)
	}

	version, err := readUVarInt(reader)
	if err != nil {
		return fmt.Errorf("metadata: read metadata version: %w", err)
	}

	switch apiKey {
	case 2: // TopicRecord
		return c.applyTopicRecord(reader)
	case 3: // PartitionRecord
		return c.applyPartitionRecord(reader, int(version))
	case 9: // RemoveTopicRecord
		return c.applyRemoveTopicRecord(reader)
	default:
		return nil
	}
}

func (c *Cluster) applyTopicRecord(r *bytes.Reader) error {
	name, err := readCompactString(r)
	if err != nil {
		return fmt.Errorf("metadata: read topic name: %w", err)
	}
	id, err := readUUID(r)
	if err != nil {
		return fmt.Errorf("metadata: read topic id: %w", err)
	}
	if err := skipTaggedFields(r); err != nil {
		return err
	}

	topic := c.ensureTopic(id)
	if topic.Name != "" && topic.Name != name {
		delete(c.topicsByName, topic.Name)
	}
	topic.Name = name
	c.topicsByName[name] = topic
	return nil
}

func (c *Cluster) applyRemoveTopicRecord(r *bytes.Reader) error {
	id, err := readUUID(r)
	if err != nil {
		return fmt.Errorf("metadata: read removed topic id: %w", err)
	}
	if err := skipTaggedFields(r); err != nil {
		return err
	}

	if topic, ok := c.topicsByID[id]; ok {
		if topic.Name != "" {
			delete(c.topicsByName, topic.Name)
		}
		delete(c.topicsByID, id)
	}
	return nil
}

func (c *Cluster) applyPartitionRecord(r *bytes.Reader, version int) error {
	partitionID, err := readInt32(r)
	if err != nil {
		return fmt.Errorf("metadata: read partition id: %w", err)
	}
	topicID, err := readUUID(r)
	if err != nil {
		return fmt.Errorf("metadata: read partition topic id: %w", err)
	}

	replicas, err := readCompactInt32Array(r)
	if err != nil {
		return fmt.Errorf("metadata: read replicas: %w", err)
	}
	isr, err := readCompactInt32Array(r)
	if err != nil {
		return fmt.Errorf("metadata: read isr: %w", err)
	}
	removing, err := readCompactInt32Array(r)
	if err != nil {
		return fmt.Errorf("metadata: read removing replicas: %w", err)
	}
	adding, err := readCompactInt32Array(r)
	if err != nil {
		return fmt.Errorf("metadata: read adding replicas: %w", err)
	}
	leader, err := readInt32(r)
	if err != nil {
		return fmt.Errorf("metadata: read leader id: %w", err)
	}
	leaderEpoch, err := readInt32(r)
	if err != nil {
		return fmt.Errorf("metadata: read leader epoch: %w", err)
	}
	partitionEpoch, err := readInt32(r)
	if err != nil {
		return fmt.Errorf("metadata: read partition epoch: %w", err)
	}

	if version >= 1 {
		if _, err := readCompactUUIDArray(r); err != nil {
			return fmt.Errorf("metadata: read directories: %w", err)
		}
	}

	offline, err := readCompactInt32Array(r)
	if err != nil {
		return fmt.Errorf("metadata: read offline replicas: %w", err)
	}

	tags, err := readTaggedFieldMap(r)
	if err != nil {
		return err
	}

	var leaderRecoveryState int8
	if raw, ok := tags[0]; ok && len(raw) > 0 {
		leaderRecoveryState = int8(raw[0])
	}

	var eligible []int32
	if raw, ok := tags[1]; ok && len(raw) > 0 {
		reader := bytes.NewReader(raw)
		eligible, err = readCompactInt32Array(reader)
		if err != nil {
			return fmt.Errorf("metadata: read eligible leader replicas: %w", err)
		}
		if reader.Len() != 0 {
			return fmt.Errorf("metadata: eligible leader replicas had %d trailing bytes", reader.Len())
		}
	}

	var lastKnown []int32
	if raw, ok := tags[2]; ok && len(raw) > 0 {
		reader := bytes.NewReader(raw)
		lastKnown, err = readCompactInt32Array(reader)
		if err != nil {
			return fmt.Errorf("metadata: read last known elr: %w", err)
		}
		if reader.Len() != 0 {
			return fmt.Errorf("metadata: last known elr had %d trailing bytes", reader.Len())
		}
	}

	topic := c.ensureTopic(topicID)
	partition := topic.ensurePartition(partitionID)
	partition.PartitionID = partitionID
	partition.LeaderID = leader
	partition.LeaderEpoch = leaderEpoch
	partition.PartitionEpoch = partitionEpoch
	partition.Replicas = replicas
	partition.ISR = isr
	partition.RemovingReplicas = removing
	partition.AddingReplicas = adding
	partition.OfflineReplicas = offline
	partition.EligibleLeaderReplicas = eligible
	partition.LastKnownEligibleLeaders = lastKnown
	partition.LeaderRecoveryState = leaderRecoveryState

	return nil
}

func (c *Cluster) ensureTopic(id [16]byte) *Topic {
	if topic, ok := c.topicsByID[id]; ok {
		return topic
	}
	topic := &Topic{
		ID:         id,
		Partitions: make(map[int32]*Partition),
	}
	c.topicsByID[id] = topic
	return topic
}

func (t *Topic) ensurePartition(id int32) *Partition {
	if partition, ok := t.Partitions[id]; ok {
		return partition
	}
	partition := &Partition{PartitionID: id}
	t.Partitions[id] = partition
	return partition
}

func readInt16(r *bytes.Reader) (int16, error) {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(buf[:])), nil
}

func readInt32(r *bytes.Reader) (int32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(buf[:])), nil
}

func readInt64(r *bytes.Reader) (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(buf[:])), nil
}

func readVarInt(r *bytes.Reader) (int64, error) {
	return binary.ReadVarint(r)
}

func readVarLong(r *bytes.Reader) (int64, error) {
	return readVarInt(r)
}

func readUVarInt(r *bytes.Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func readUUID(r *bytes.Reader) ([16]byte, error) {
	var id [16]byte
	if _, err := io.ReadFull(r, id[:]); err != nil {
		return [16]byte{}, err
	}
	return id, nil
}

func skipN(r *bytes.Reader, n int64) error {
	if n == 0 {
		return nil
	}
	if n < 0 {
		return fmt.Errorf("metadata: negative skip %d", n)
	}
	if _, err := r.Seek(n, io.SeekCurrent); err != nil {
		return err
	}
	return nil
}

func readCompactString(r *bytes.Reader) (string, error) {
	lengthPlusOne, err := readUVarInt(r)
	if err != nil {
		return "", err
	}
	if lengthPlusOne == 0 {
		return "", fmt.Errorf("metadata: compact string null not supported")
	}
	length := int(lengthPlusOne - 1)
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readCompactInt32Array(r *bytes.Reader) ([]int32, error) {
	lengthPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, err
	}
	if lengthPlusOne == 0 {
		return nil, fmt.Errorf("metadata: compact array length zero indicates null")
	}
	length := int(lengthPlusOne - 1)
	if length == 0 {
		return []int32{}, nil
	}
	result := make([]int32, length)
	for i := 0; i < length; i++ {
		v, err := readInt32(r)
		if err != nil {
			return nil, err
		}
		result[i] = v
	}
	return result, nil
}

func readCompactUUIDArray(r *bytes.Reader) ([][16]byte, error) {
	lengthPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, err
	}
	if lengthPlusOne == 0 {
		return nil, fmt.Errorf("metadata: compact uuid array length zero indicates null")
	}
	length := int(lengthPlusOne - 1)
	if length == 0 {
		return [][16]byte{}, nil
	}
	result := make([][16]byte, length)
	for i := 0; i < length; i++ {
		id, err := readUUID(r)
		if err != nil {
			return nil, err
		}
		result[i] = id
	}
	return result, nil
}

func readTaggedFieldMap(r *bytes.Reader) (map[uint32][]byte, error) {
	countPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, err
	}
	if countPlusOne == 0 {
		return map[uint32][]byte{}, nil
	}
	count := int(countPlusOne - 1)
	out := make(map[uint32][]byte, count)
	for i := 0; i < count; i++ {
		tag, err := readUVarInt(r)
		if err != nil {
			return nil, err
		}
		sizePlusOne, err := readUVarInt(r)
		if err != nil {
			return nil, err
		}
		if sizePlusOne == 0 {
			return nil, fmt.Errorf("metadata: tagged field %d had zero length", tag)
		}
		size := int(sizePlusOne - 1)
		buf := make([]byte, size)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out[uint32(tag)] = buf
	}
	return out, nil
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
			return fmt.Errorf("metadata: tagged field length zero")
		}
		if err := skipN(r, int64(sizePlusOne-1)); err != nil {
			return err
		}
	}
	return nil
}
