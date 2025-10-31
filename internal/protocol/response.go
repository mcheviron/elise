// Package protocol contains Kafka wire-format request/response helpers.
package protocol

import (
	"bytes"
	"encoding/binary"
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
