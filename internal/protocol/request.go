package protocol

import (
	"bytes"
	"encoding/binary"
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

	header, err := parseRequestHeaderV2(reader)
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

func parseRequestHeaderV2(r *bytes.Reader) (RequestHeaderV2, error) {
	apiKey, err := readInt16(r)
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

	clientID, err := readNullableString(r)
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("protocol: reading client id: %w", err)
	}

	taggedFields, err := readTaggedFields(r)
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("protocol: reading tagged fields: %w", err)
	}

	return RequestHeaderV2{
		APIKey:        APIKey(apiKey),
		APIVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
		TaggedFields:  taggedFields,
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

func readInt16(r *bytes.Reader) (int16, error) {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(buf[:])), nil
}

func readInt32(r *bytes.Reader) (int32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(buf[:])), nil
}

func readNullableString(r *bytes.Reader) (string, error) {
	length, err := readInt16(r)
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", nil
	}
	if length == 0 {
		return "", nil
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	str := string(buf)
	return str, nil
}

func readUVarInt(r *bytes.Reader) (uint64, error) {
	value, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	return value, nil
}
