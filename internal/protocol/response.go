// Package protocol contains Kafka wire-format request/response helpers.
package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ResponseHeaderV0 encodes the response header version 0.
type ResponseHeaderV0 struct {
	CorrelationID int32
}

// ApiVersionsResponseV4 represents the body of an ApiVersions v4 response.
type APIVersionsResponseV4 struct {
	ErrorCode      ErrorCode
	APIVersions    []APIVersionRange
	ThrottleTimeMS int32
	ResponseTags   []TaggedField
	APIVersionTags map[APIKey][]TaggedField
}

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

// DescribeTopicPartitionsResponsePartition models partition-level information. Currently unused.
type DescribeTopicPartitionsResponsePartition struct{}

// Encode serializes the ApiVersions v4 response body.
func (r APIVersionsResponseV4) Encode() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 16))

	writeInt16(buf, int16(r.ErrorCode))

	writeCompactArrayLen(buf, len(r.APIVersions))
	for _, entry := range r.APIVersions {
		writeInt16(buf, int16(entry.APIKey))
		writeInt16(buf, entry.MinVersion)
		writeInt16(buf, entry.MaxVersion)
		writeTaggedFields(buf, r.APIVersionTags[entry.APIKey])
	}

	writeInt32(buf, r.ThrottleTimeMS)
	writeTaggedFields(buf, r.ResponseTags)

	return buf.Bytes()
}

// Encode serializes the DescribeTopicPartitions response body.
func (r DescribeTopicPartitionsResponse) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))

	writeInt32(buf, r.ThrottleTimeMS)

	writeCompactArrayLen(buf, len(r.Topics))
	for i, topic := range r.Topics {
		if err := encodeDescribeTopicPartitionsTopic(buf, topic); err != nil {
			return nil, wrapIndexError("topic", i, err)
		}
	}

	if err := writeCompactNullableCursor(buf, r.NextCursor); err != nil {
		return nil, err
	}

	writeTaggedFields(buf, r.TaggedFields)

	return buf.Bytes(), nil
}

// EncodeResponse constructs the byte representation of a response that uses
// header v0 followed by the provided body payload.
func EncodeResponse(header ResponseHeaderV0, body []byte) []byte {
	const headerLen = 4
	size := headerLen + len(body)
	frame := make([]byte, 4+size)
	binary.BigEndian.PutUint32(frame[0:4], uint32(size))
	binary.BigEndian.PutUint32(frame[4:8], uint32(header.CorrelationID))
	copy(frame[8:], body)
	return frame
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
	if len(partitions) != 0 {
		return fmt.Errorf("protocol: non-empty partitions not supported yet")
	}
	writeCompactArrayLen(buf, 0)
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

func wrapIndexError(kind string, index int, err error) error {
	return fmt.Errorf("protocol: encoding %s[%d]: %w", kind, index, err)
}
