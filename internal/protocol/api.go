// Package protocol contains Kafka wire-format request/response encoders and decoders.
package protocol

// APIKey identifies a Kafka admin/data plane API.
type APIKey int16

const (
	// APIKeyFetch corresponds to the Fetch RPC.
	APIKeyFetch APIKey = 1
	// APIKeyAPIVersions corresponds to the ApiVersions RPC.
	APIKeyAPIVersions APIKey = 18
	// APIKeyDescribeTopicPartitions corresponds to DescribeTopicPartitions RPC.
	APIKeyDescribeTopicPartitions APIKey = 75
)

// SupportedRange captures the inclusive version span supported for an API.
type SupportedRange struct {
	Min int16
	Max int16
}

// APIVersionRange captures the version span associated with a specific API key.
type APIVersionRange struct {
	APIKey     APIKey
	MinVersion int16
	MaxVersion int16
}

// APIVersionsSupportedRange defines this broker's supported API Versions range.
var APIVersionsSupportedRange = SupportedRange{
	Min: 0,
	Max: 4,
}

// SupportedAPIs enumerates the APIs exposed by this broker.
var SupportedAPIs = []APIVersionRange{
	{
		APIKey:     APIKeyFetch,
		MinVersion: 0,
		MaxVersion: 16,
	},
	{
		APIKey:     APIKeyAPIVersions,
		MinVersion: APIVersionsSupportedRange.Min,
		MaxVersion: APIVersionsSupportedRange.Max,
	},
	{
		APIKey:     APIKeyDescribeTopicPartitions,
		MinVersion: 0,
		MaxVersion: 0,
	},
}

// Contains reports whether v lies within the supported range.
func (r SupportedRange) Contains(v int16) bool {
	return v >= r.Min && v <= r.Max
}

// IsFlexibleRequest reports whether the given API key/version pair uses the flexible request header.
func IsFlexibleRequest(key APIKey, version int16) bool {
	switch key {
	case APIKeyFetch:
		return version >= 12
	case APIKeyDescribeTopicPartitions:
		return true
	default:
		return false
	}
}

// ErrorCode models Kafka error codes returned in responses.
type ErrorCode int16

const (
	// ErrorCodeNone indicates success.
	ErrorCodeNone ErrorCode = 0
	// ErrorCodeUnsupportedVersion indicates the request used an unsupported API version.
	ErrorCodeUnsupportedVersion ErrorCode = 35
	// ErrorCodeUnknownTopicOrPartition indicates the requested topic or partition does not exist.
	ErrorCodeUnknownTopicOrPartition ErrorCode = 3
	// ErrorCodeUnknownTopicID indicates the requested topic ID is unknown to the broker.
	ErrorCodeUnknownTopicID ErrorCode = 100
)
