package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// DescribeTopicPartitionsResponse represents the body of a DescribeTopicPartitions v0 response.
type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMS int32
	Topics         []DescribeTopicPartitionsResponseTopic
	NextCursor     *DescribeTopicPartitionsCursor
	TaggedFields   []TaggedField
}

// DescribeTopicPartitionsResponseTopic models topic-level information in the response.
type DescribeTopicPartitionsResponseTopic struct {
	ErrorCode                 ErrorCode
	Name                      *string
	TopicID                   [16]byte
	IsInternal                bool
	Partitions                []DescribeTopicPartitionsResponsePartition
	NextCursor                *DescribeTopicPartitionsCursor
	TopicAuthorizedOperations int32
	TaggedFields              []TaggedField
}

// DescribeTopicPartitionsResponsePartition models partition-level information in the response.
type DescribeTopicPartitionsResponsePartition struct {
	ErrorCode              ErrorCode
	PartitionIndex         int32
	LeaderID               int32
	LeaderEpoch            int32
	ReplicaNodes           []int32
	IsrNodes               []int32
	EligibleLeaderReplicas []int32
	LastKnownElr           []int32
	OfflineReplicas        []int32
	TaggedFields           []TaggedField
}

// ErrorCode mirrors Kafka error codes used in responses.
type ErrorCode int16

const (
	// ErrorCodeNone indicates success.
	ErrorCodeNone ErrorCode = 0
	// ErrorCodeUnknownTopicOrPartition indicates the requested topic or partition does not exist.
	ErrorCodeUnknownTopicOrPartition ErrorCode = 3
)

// Encode serializes the DescribeTopicPartitions response body.
func (r DescribeTopicPartitionsResponse) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))

	writeInt32(buf, r.ThrottleTimeMS)

	writeCompactArrayLen(buf, len(r.Topics))
	for i, topic := range r.Topics {
		if err := encodeDescribeTopicPartitionsTopic(buf, topic); err != nil {
			return nil, fmt.Errorf("protocol: encoding topic[%d]: %w", i, err)
		}
	}

	if err := writeCompactNullableCursor(buf, r.NextCursor); err != nil {
		return nil, err
	}

	writeTaggedFields(buf, r.TaggedFields)

	return buf.Bytes(), nil
}

func encodeDescribeTopicPartitionsTopic(buf *bytes.Buffer, topic DescribeTopicPartitionsResponseTopic) error {
	writeInt16(buf, int16(topic.ErrorCode))
	writeCompactNullableString(buf, topic.Name)
	writeUUID(buf, topic.TopicID)
	writeBool(buf, topic.IsInternal)

	if err := encodeDescribeTopicPartitionsPartitions(buf, topic.Partitions); err != nil {
		return err
	}

	if err := writeCompactNullableCursor(buf, topic.NextCursor); err != nil {
		return err
	}

	writeInt32(buf, topic.TopicAuthorizedOperations)
	writeTaggedFields(buf, topic.TaggedFields)
	return nil
}

func encodeDescribeTopicPartitionsPartitions(buf *bytes.Buffer, partitions []DescribeTopicPartitionsResponsePartition) error {
	writeCompactArrayLen(buf, len(partitions))
	for _, partition := range partitions {
		writeInt16(buf, int16(partition.ErrorCode))
		writeInt32(buf, partition.PartitionIndex)
		writeInt32(buf, partition.LeaderID)
		writeInt32(buf, partition.LeaderEpoch)
		writeCompactInt32Array(buf, partition.ReplicaNodes)
		writeCompactInt32Array(buf, partition.IsrNodes)
		writeCompactNullableInt32Array(buf, partition.EligibleLeaderReplicas)
		writeCompactNullableInt32Array(buf, partition.LastKnownElr)
		writeCompactInt32Array(buf, partition.OfflineReplicas)
		writeTaggedFields(buf, partition.TaggedFields)
	}
	return nil
}

func writeCompactNullableCursor(buf *bytes.Buffer, cursor *DescribeTopicPartitionsCursor) error {
	if cursor == nil {
		writeUVarInt(buf, 0)
		return nil
	}

	inner := bytes.NewBuffer(make([]byte, 0, 32))
	writeCompactString(inner, cursor.TopicName)
	writeInt32(inner, cursor.PartitionIndex)
	writeTaggedFields(inner, cursor.TaggedFields)

	payload := inner.Bytes()
	writeUVarInt(buf, uint64(len(payload)+1))
	buf.Write(payload)
	return nil
}

func writeCompactInt32Array(buf *bytes.Buffer, values []int32) {
	writeUVarInt(buf, uint64(len(values)+1))
	for _, v := range values {
		writeInt32(buf, v)
	}
}

func writeCompactNullableInt32Array(buf *bytes.Buffer, values []int32) {
	if values == nil {
		writeUVarInt(buf, 0)
		return
	}
	writeCompactInt32Array(buf, values)
}

func writeTaggedFields(buf *bytes.Buffer, fields []TaggedField) {
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

func writeBool(buf *bytes.Buffer, v bool) {
	if v {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}

func writeUVarInt(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}
