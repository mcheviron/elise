package protocol

import (
	"bytes"
	"fmt"
)

// FetchRequestV16 captures the subset of Fetch v16 request fields needed by the broker.
type FetchRequestV16 struct {
	Topics []FetchRequestTopic
}

// FetchRequestTopic represents a topic entry in the Fetch request.
type FetchRequestTopic struct {
	TopicID    [16]byte
	Partitions []FetchRequestPartition
}

// FetchRequestPartition represents a partition entry in the Fetch request.
type FetchRequestPartition struct {
	PartitionIndex int32
}

// ParseFetchRequestV16 decodes a Fetch v16 request body.
func ParseFetchRequestV16(payload []byte) (*FetchRequestV16, error) {
	reader := bytes.NewReader(payload)

	// Skip fields the broker does not currently use but which are present in the
	// wire representation in order.
	if _, err := readInt32(reader); err != nil {
		return nil, fmt.Errorf("protocol: reading max_wait_ms: %w", err)
	}
	if _, err := readInt32(reader); err != nil {
		return nil, fmt.Errorf("protocol: reading min_bytes: %w", err)
	}
	if _, err := readInt32(reader); err != nil {
		return nil, fmt.Errorf("protocol: reading max_bytes: %w", err)
	}
	if _, err := readInt8(reader); err != nil {
		return nil, fmt.Errorf("protocol: reading isolation_level: %w", err)
	}
	if _, err := readInt32(reader); err != nil {
		return nil, fmt.Errorf("protocol: reading session_id: %w", err)
	}
	if _, err := readInt32(reader); err != nil {
		return nil, fmt.Errorf("protocol: reading session_epoch: %w", err)
	}

	topics, err := readFetchRequestTopics(reader)
	if err != nil {
		return nil, err
	}

	if err := skipFetchRequestForgottenTopics(reader); err != nil {
		return nil, err
	}

	if _, err := readCompactString(reader); err != nil {
		return nil, fmt.Errorf("protocol: reading rack_id: %w", err)
	}

	if _, err := readTaggedFields(reader); err != nil {
		return nil, fmt.Errorf("protocol: reading request tagged fields: %w", err)
	}

	if reader.Len() != 0 {
		return nil, fmt.Errorf("protocol: fetch request has %d trailing bytes", reader.Len())
	}

	return &FetchRequestV16{Topics: topics}, nil
}

func readFetchRequestTopics(r *bytes.Reader) ([]FetchRequestTopic, error) {
	count, err := readCompactArrayLen(r)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading topics length: %w", err)
	}

	items := make([]FetchRequestTopic, 0, count)
	for i := 0; i < count; i++ {
		topicID, err := readUUID(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] id: %w", i, err)
		}

		partitions, err := readFetchRequestPartitions(r, i)
		if err != nil {
			return nil, err
		}

		if _, err := readTaggedFields(r); err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] tagged fields: %w", i, err)
		}

		items = append(items, FetchRequestTopic{
			TopicID:    topicID,
			Partitions: partitions,
		})
	}
	return items, nil
}

func readFetchRequestPartitions(r *bytes.Reader, topicIndex int) ([]FetchRequestPartition, error) {
	count, err := readCompactArrayLen(r)
	if err != nil {
		return nil, fmt.Errorf("protocol: reading topic[%d] partitions length: %w", topicIndex, err)
	}

	items := make([]FetchRequestPartition, 0, count)
	for j := 0; j < count; j++ {
		partitionIndex, err := readInt32(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] partition[%d] index: %w", topicIndex, j, err)
		}

		if _, err := readInt32(r); err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] partition[%d] current_leader_epoch: %w", topicIndex, j, err)
		}

		if _, err := readInt64(r); err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] partition[%d] fetch_offset: %w", topicIndex, j, err)
		}

		if _, err := readInt32(r); err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] partition[%d] last_fetched_epoch: %w", topicIndex, j, err)
		}

		if _, err := readInt64(r); err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] partition[%d] log_start_offset: %w", topicIndex, j, err)
		}

		if _, err := readInt32(r); err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] partition[%d] partition_max_bytes: %w", topicIndex, j, err)
		}

		if _, err := readTaggedFields(r); err != nil {
			return nil, fmt.Errorf("protocol: reading topic[%d] partition[%d] tagged fields: %w", topicIndex, j, err)
		}

		items = append(items, FetchRequestPartition{PartitionIndex: partitionIndex})
	}
	return items, nil
}

func skipFetchRequestForgottenTopics(r *bytes.Reader) error {
	count, err := readCompactArrayLen(r)
	if err != nil {
		return fmt.Errorf("protocol: reading forgotten topics length: %w", err)
	}
	for i := 0; i < count; i++ {
		if _, err := readUUID(r); err != nil {
			return fmt.Errorf("protocol: reading forgotten topic[%d] id: %w", i, err)
		}

		partitionCount, err := readCompactArrayLen(r)
		if err != nil {
			return fmt.Errorf("protocol: reading forgotten topic[%d] partitions length: %w", i, err)
		}
		for j := 0; j < partitionCount; j++ {
			if _, err := readInt32(r); err != nil {
				return fmt.Errorf("protocol: reading forgotten topic[%d] partition[%d]: %w", i, j, err)
			}
		}

		if _, err := readTaggedFields(r); err != nil {
			return fmt.Errorf("protocol: reading forgotten topic[%d] tagged fields: %w", i, err)
		}
	}
	return nil
}
