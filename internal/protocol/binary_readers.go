// Package protocol contains Kafka wire-format request/response helpers.
package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

var errCompactStringNull = errors.New("protocol: compact string length zero denotes null")

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
	return string(buf), nil
}

func readCompactString(r *bytes.Reader) (string, error) {
	lengthPlusOne, err := readUVarInt(r)
	if err != nil {
		return "", err
	}
	if lengthPlusOne == 0 {
		return "", errCompactStringNull
	}
	length := int(lengthPlusOne - 1)
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readCompactNullableString(r *bytes.Reader) (*string, error) {
	lengthPlusOne, err := readUVarInt(r)
	if err != nil {
		return nil, err
	}
	if lengthPlusOne == 0 {
		return nil, nil
	}
	length := int(lengthPlusOne - 1)
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	str := string(buf)
	return &str, nil
}

func readUVarInt(r *bytes.Reader) (uint64, error) {
	value, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func readUUID(r *bytes.Reader) ([16]byte, error) {
	var id [16]byte
	if _, err := io.ReadFull(r, id[:]); err != nil {
		return [16]byte{}, err
	}
	return id, nil
}
