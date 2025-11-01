package protocol

import "bytes"

// FetchResponseV16 models the body of a Fetch v16 response.
type FetchResponseV16 struct {
	ThrottleTimeMS int32
	ErrorCode      ErrorCode
	SessionID      int32
	Responses      []FetchableTopicResponse
	NodeEndpoints  []FetchResponseNodeEndpoint
	TaggedFields   []TaggedField
}

// FetchableTopicResponse represents per-topic information in the Fetch response.
// Fields needed for non-empty responses will be filled in future stages.
type FetchableTopicResponse struct{}

// FetchResponseNodeEndpoint encodes leader endpoint information for Fetch responses.
// The implementation will be completed in later stages alongside topic data.
type FetchResponseNodeEndpoint struct{}

// Encode serializes the Fetch v16 response body.
func (r FetchResponseV16) Encode() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 32))

	writeInt32(buf, r.ThrottleTimeMS)
	writeInt16(buf, int16(r.ErrorCode))
	writeInt32(buf, r.SessionID)

	writeCompactArrayLen(buf, len(r.Responses))
	if len(r.Responses) > 0 {
		panic("FetchResponseV16 encoding for non-empty responses is not implemented yet")
	}

	writeTaggedFields(buf, r.TaggedFields)

	return buf.Bytes()
}
