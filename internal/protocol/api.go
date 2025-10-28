package protocol

// APIKey identifies a Kafka admin/data plane API.
type APIKey int16

const (
	// APIKeyApiVersions corresponds to the ApiVersions RPC.
	APIKeyApiVersions APIKey = 18
)

// SupportedRange captures the inclusive version span supported for an API.
type SupportedRange struct {
	Min int16
	Max int16
}

// ApiVersionsSupportedRange defines this broker's supported ApiVersions range.
var ApiVersionsSupportedRange = SupportedRange{
	Min: 0,
	Max: 4,
}

// Contains reports whether v lies within the supported range.
func (r SupportedRange) Contains(v int16) bool {
	return v >= r.Min && v <= r.Max
}

// ErrorCode models Kafka error codes returned in responses.
type ErrorCode int16

const (
	// ErrorCodeNone indicates success.
	ErrorCodeNone ErrorCode = 0
	// ErrorCodeUnsupportedVersion indicates the request used an unsupported API version.
	ErrorCodeUnsupportedVersion ErrorCode = 35
)
