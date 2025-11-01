// Package protocol contains Kafka wire-format request/response helpers.
package protocol

import (
	"bytes"
	"fmt"
	"io"
)

// Request models the Kafka request envelope containing the message size,
// header fields, and the remaining raw body bytes.
type Request struct {
	MessageSize int32
	Header      RequestHeaderV2
	Body        []byte
}

// RequestHeaderV2 captures the fields for Kafka request header version 2.
type RequestHeaderV2 struct {
	APIKey        APIKey
	APIVersion    int16
	CorrelationID int32
	ClientID      string
	TaggedFields  []TaggedField
	Flexible      bool
}

// TaggedField represents a flexible version tagged field entry.
type TaggedField struct {
	Tag   uint32
	Value []byte
}

// ParseRequest constructs a Request from the provided message size and raw payload.
func ParseRequest(messageSize int32, payload []byte) (*Request, error) {
	if int64(len(payload)) != int64(messageSize) {
		return nil, fmt.Errorf("protocol: payload length %d does not match message size %d", len(payload), messageSize)
	}

	reader := bytes.NewReader(payload)

	header, err := parseRequestHeader(reader)
	if err != nil {
		return nil, err
	}

	body := make([]byte, reader.Len())
	if _, err := io.ReadFull(reader, body); err != nil && err != io.EOF {
		return nil, fmt.Errorf("protocol: reading body: %w", err)
	}

	return &Request{
		MessageSize: messageSize,
		Header:      header,
		Body:        body,
	}, nil
}

func parseRequestHeader(r *bytes.Reader) (RequestHeaderV2, error) {
	rawKey, err := readInt16(r)
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("protocol: reading api key: %w", err)
	}

	apiVersion, err := readInt16(r)
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("protocol: reading api version: %w", err)
	}

	correlationID, err := readInt32(r)
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("protocol: reading correlation id: %w", err)
	}

	apiKey := APIKey(rawKey)
	flexible := isFlexibleRequest(apiKey, apiVersion)

	var clientID string
	var taggedFields []TaggedField

	if flexible {
		clientPtr, err := readCompactNullableString(r)
		if err != nil {
			return RequestHeaderV2{}, fmt.Errorf("protocol: reading client id: %w", err)
		}
		if clientPtr != nil {
			clientID = *clientPtr
		}

		taggedFields, err = readTaggedFields(r)
		if err != nil {
			return RequestHeaderV2{}, fmt.Errorf("protocol: reading tagged fields: %w", err)
		}
	} else {
		var err error
		clientID, err = readNullableString(r)
		if err != nil {
			return RequestHeaderV2{}, fmt.Errorf("protocol: reading client id: %w", err)
		}
		taggedFields = nil
	}

	return RequestHeaderV2{
		APIKey:        apiKey,
		APIVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
		TaggedFields:  taggedFields,
		Flexible:      flexible,
	}, nil
}

func readTaggedFields(r *bytes.Reader) ([]TaggedField, error) {
	countPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, err
	}
	if countPlusOne == 0 {
		return nil, nil
	}

	fieldCount := int(countPlusOne - 1)
	fields := make([]TaggedField, 0, fieldCount)

	for i := range fieldCount {
		tag, err := readUVarInt(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading tag index %d: %w", i, err)
		}

		sizePlusOne, err := readUVarInt(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: reading tag %d size: %w", tag, err)
		}
		if sizePlusOne == 0 {
			return nil, fmt.Errorf("protocol: tagged field %d has null value", tag)
		}

		size := int(sizePlusOne - 1)
		value := make([]byte, size)
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, fmt.Errorf("protocol: reading tag %d value: %w", tag, err)
		}

		fields = append(fields, TaggedField{
			Tag:   uint32(tag),
			Value: value,
		})
	}

	return fields, nil
}
