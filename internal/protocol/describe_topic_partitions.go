// Package protocol contains Kafka wire-format request/response helpers.
package protocol

import (
	"bytes"
	"fmt"
	"io"
)

// DescribeTopicPartitionsRequest represents the parsed request body for the DescribeTopicPartitions API.
type DescribeTopicPartitionsRequest struct {
	Topics                 []DescribeTopicPartitionsRequestTopic
	ResponsePartitionLimit int32
	Cursor                 *DescribeTopicPartitionsCursor
	TaggedFields           []TaggedField
}

// DescribeTopicPartitionsRequestTopic captures a single topic entry in the request.
type DescribeTopicPartitionsRequestTopic struct {
	Name         string
	TopicID      [16]byte
	Partitions   []DescribeTopicPartitionsRequestPartition
	TaggedFields []TaggedField
}

// DescribeTopicPartitionsRequestPartition captures requested partition indexes.
type DescribeTopicPartitionsRequestPartition struct {
	PartitionIndex int32
	TaggedFields   []TaggedField
}

// DescribeTopicPartitionsCursor models the cursor structure used by both requests and responses.
type DescribeTopicPartitionsCursor struct {
	TopicName      string
	PartitionIndex int32
	TaggedFields   []TaggedField
}

// ParseDescribeTopicPartitionsRequest parses the request body into the strongly typed representation.
func ParseDescribeTopicPartitionsRequest(payload []byte) (*DescribeTopicPartitionsRequest, error) {
	reader := bytes.NewReader(payload)

	topics, err := readDescribeTopicPartitionsTopics(reader)
	if err != nil {
		return nil, err
	}

	limit, err := readInt32(reader)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading response_partition_limit: %w", err)
	}

	cursor, err := readDescribeTopicPartitionsCursor(reader)
	if err != nil {
		return nil, err
	}

	taggedFields, err := readTaggedFields(reader)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading request tagged fields: %w", err)
	}

	if reader.Len() != 0 {
		return nil, fmt.Errorf("protocol: describe topic partitions request has %d trailing bytes", reader.Len())
	}

	return &DescribeTopicPartitionsRequest{
		Topics:                 topics,
		ResponsePartitionLimit: limit,
		Cursor:                 cursor,
		TaggedFields:           taggedFields,
	}, nil
}

func readDescribeTopicPartitionsTopics(r *bytes.Reader) ([]DescribeTopicPartitionsRequestTopic, error) {
	countPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading topics length: %w", err)
	}
	if countPlusOne == 0 {
		return nil, nil
	}
	count := int(countPlusOne - 1)

	items := make([]DescribeTopicPartitionsRequestTopic, 0, count)
	for i := range count {
		namePtr, err := readCompactNullableString(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] name: %w", i, err)
		}
		name := ""
		if namePtr != nil {
			name = *namePtr
		}

		topicID, err := readUUID(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] id: %w", i, err)
		}

		partitions, err := readDescribeTopicPartitionsPartitions(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] partitions: %w", i, err)
		}

		tagged, err := readTaggedFields(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] tagged fields: %w", i, err)
		}
		items = append(items, DescribeTopicPartitionsRequestTopic{
			Name:         name,
			TopicID:      topicID,
			Partitions:   partitions,
			TaggedFields: tagged,
		})
	}
	return items, nil
}

func readDescribeTopicPartitionsPartitions(r *bytes.Reader) ([]DescribeTopicPartitionsRequestPartition, error) {
	countPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading partitions length: %w", err)
	}
	if countPlusOne == 0 {
		return nil, nil
	}
	count := int(countPlusOne - 1)

	items := make([]DescribeTopicPartitionsRequestPartition, 0, count)
	for i := range count {
		index, err := readInt32(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading partitions[%d] index: %w", i, err)
		}
		tagged, err := readTaggedFields(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading partitions[%d] tagged fields: %w", i, err)
		}
		items = append(items, DescribeTopicPartitionsRequestPartition{
			PartitionIndex: index,
			TaggedFields:   tagged,
		})
	}
	return items, nil
}

func readDescribeTopicPartitionsCursor(r *bytes.Reader) (*DescribeTopicPartitionsCursor, error) {
	sizePlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading cursor length: %w", err)
	}
	if sizePlusOne == 0 {
		return nil, nil
	}

	size := int(sizePlusOne - 1)
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("protocol: reading cursor payload: %w", err)
	}

	sub := bytes.NewReader(buf)
	name, err := readCompactString(sub)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading cursor topic name: %w", err)
	}
	partitionIndex, err := readInt32(sub)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading cursor partition index: %w", err)
	}
	tagged, err := readTaggedFields(sub)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading cursor tagged fields: %w", err)
	}
	if sub.Len() != 0 {
		return nil, fmt.Errorf("protocol: cursor has %d trailing bytes", sub.Len())
	}

	return &DescribeTopicPartitionsCursor{
		TopicName:      name,
		PartitionIndex: partitionIndex,
		TaggedFields:   tagged,
	}, nil
}
