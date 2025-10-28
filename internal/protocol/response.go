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
type ApiVersionsResponseV4 struct {
	ErrorCode      ErrorCode
	APIVersions    []APIVersionRange
	ThrottleTimeMS int32
	ResponseTags   []TaggedField
	APIVersionTags map[APIKey][]TaggedField
}

// Encode serializes the ApiVersions v4 response body.
func (r ApiVersionsResponseV4) Encode() []byte {
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

func writeUVarInt(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}
