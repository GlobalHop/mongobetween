package mongo

import (
	"encoding/binary"

	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

// MaxWireMessageSize is the largest wire message the proxy accepts: maxBsonObjectSize plus MongoDB's 16KiB command overhead.
const MaxWireMessageSize = 48000000 + 16*1024

const (
	wireHeaderSize = 16
	opMsgFlagsEnd  = wireHeaderSize + 4
	checksumSize   = 4
)

// StripExhaustAllowed returns wm untouched unless it is an OP_MSG with exhaustAllowed set. In that case it returns a
// copy with the bit cleared (and the trailing checksum dropped if present), so the upstream never streams replies.
func StripExhaustAllowed(wm []byte) []byte {
	if len(wm) < opMsgFlagsEnd || wiremessage.OpCode(binary.LittleEndian.Uint32(wm[12:16])) != wiremessage.OpMsg {
		return wm
	}
	flags := wiremessage.MsgFlag(binary.LittleEndian.Uint32(wm[wireHeaderSize:opMsgFlagsEnd]))
	if flags&wiremessage.ExhaustAllowed == 0 {
		return wm
	}

	out := make([]byte, len(wm))
	copy(out, wm)
	flags &^= wiremessage.ExhaustAllowed
	if flags&wiremessage.ChecksumPresent != 0 && len(out) >= opMsgFlagsEnd+checksumSize {
		flags &^= wiremessage.ChecksumPresent
		out = out[:len(out)-checksumSize]
		binary.LittleEndian.PutUint32(out[0:4], uint32(len(out))) // #nosec G115
	}
	binary.LittleEndian.PutUint32(out[wireHeaderSize:opMsgFlagsEnd], uint32(flags))
	return out
}

// hasMoreToCome reports whether an OP_MSG reply announces further streamed replies.
func hasMoreToCome(wm []byte) bool {
	if len(wm) < opMsgFlagsEnd || wiremessage.OpCode(binary.LittleEndian.Uint32(wm[12:16])) != wiremessage.OpMsg {
		return false
	}
	return wiremessage.MsgFlag(binary.LittleEndian.Uint32(wm[wireHeaderSize:opMsgFlagsEnd]))&wiremessage.MoreToCome != 0
}
