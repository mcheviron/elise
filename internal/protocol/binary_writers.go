// Package protocol contains Kafka wire-format request/response helpers.
package protocol

import (
	"bytes"
	"encoding/binary"
)

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

func writeInt64(buf *bytes.Buffer, v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	buf.Write(tmp[:])
}

func writeCompactArrayLen(buf *bytes.Buffer, length int) {
	writeUVarInt(buf, uint64(length+1))
}

func writeUUID(buf *bytes.Buffer, id [16]byte) {
	buf.Write(id[:])
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

func writeCompactNullableBytes(buf *bytes.Buffer, data []byte) {
	if data == nil {
		writeUVarInt(buf, 0)
		return
	}
	writeUVarInt(buf, uint64(len(data)+1))
	if len(data) > 0 {
		buf.Write(data)
	}
}
