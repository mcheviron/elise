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

func writeCompactString(buf *bytes.Buffer, value string) {
	writeUVarInt(buf, uint64(len(value)+1))
	if len(value) > 0 {
		buf.WriteString(value)
	}
}

func writeCompactNullableString(buf *bytes.Buffer, value *string) {
	if value == nil {
		writeUVarInt(buf, 0)
		return
	}
	writeCompactString(buf, *value)
}

func writeUUID(buf *bytes.Buffer, id [16]byte) {
	buf.Write(id[:])
}

func writeBool(buf *bytes.Buffer, v bool) {
	if v {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}
