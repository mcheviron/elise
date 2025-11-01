package protocol

import "bytes"

// FetchResponseV16 models the body of a Fetch v16 response.
type FetchResponseV16 struct {
	ThrottleTimeMS int32
	ErrorCode      ErrorCode
	SessionID      int32
	Responses      []FetchableTopicResponse
	TaggedFields   []TaggedField
}

// FetchableTopicResponse represents per-topic information in the Fetch response.
type FetchableTopicResponse struct {
	TopicID      [16]byte
	Partitions   []FetchablePartitionResponse
	TaggedFields []TaggedField
}

// FetchablePartitionResponse represents per-partition information in the Fetch response.
type FetchablePartitionResponse struct {
	PartitionIndex       int32
	ErrorCode            ErrorCode
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []FetchResponseAbortedTransaction
	PreferredReadReplica int32
	Records              []byte
	TaggedFields         []TaggedField
}

// FetchResponseAbortedTransaction represents entries in the aborted transactions array.
type FetchResponseAbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

// Encode serializes the Fetch v16 response body.
func (r FetchResponseV16) Encode() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 64))

	writeInt32(buf, r.ThrottleTimeMS)
	writeInt16(buf, int16(r.ErrorCode))
	writeInt32(buf, r.SessionID)

	writeCompactArrayLen(buf, len(r.Responses))
	for _, topic := range r.Responses {
		writeUUID(buf, topic.TopicID)
		writeCompactArrayLen(buf, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			writeInt32(buf, partition.PartitionIndex)
			writeInt16(buf, int16(partition.ErrorCode))
			writeInt64(buf, partition.HighWatermark)
			writeInt64(buf, partition.LastStableOffset)
			writeInt64(buf, partition.LogStartOffset)

			if partition.AbortedTransactions == nil {
				writeUVarInt(buf, 0)
			} else {
				writeUVarInt(buf, uint64(len(partition.AbortedTransactions)+1))
				for _, aborted := range partition.AbortedTransactions {
					writeInt64(buf, aborted.ProducerID)
					writeInt64(buf, aborted.FirstOffset)
				}
			}

			writeInt32(buf, partition.PreferredReadReplica)
			writeCompactNullableBytes(buf, partition.Records)
			writeTaggedFields(buf, partition.TaggedFields)
		}
		writeTaggedFields(buf, topic.TaggedFields)
	}

	writeTaggedFields(buf, r.TaggedFields)

	return buf.Bytes()
}
