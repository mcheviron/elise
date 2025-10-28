package protocol

import "encoding/binary"

// ResponseHeaderV0 encodes the response header version 0.
type ResponseHeaderV0 struct {
	CorrelationID int32
}

// ApiVersionsResponseV4 represents the body of an ApiVersions v4 response.
// In this stage we only need the error code field.
type ApiVersionsResponseV4 struct {
	ErrorCode ErrorCode
}

// Encode serializes the ApiVersions response body (currently only error_code).
func (r ApiVersionsResponseV4) Encode() []byte {
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body, uint16(r.ErrorCode))
	return body
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
