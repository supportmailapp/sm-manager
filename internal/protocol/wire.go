package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// maxFrameLen caps a single frame to guard against a malformed/hostile length
// prefix allocating unbounded memory.
const maxFrameLen = 16 << 20 // 16 MiB

// ReadFrame reads one length-prefixed frame: a 4-byte big-endian uint32 length
// followed by that many bytes of JSON payload.
func ReadFrame(r *bufio.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return nil, fmt.Errorf("zero-length frame")
	}
	if n > maxFrameLen {
		return nil, fmt.Errorf("frame too large: %d bytes", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// EncodeFrame prefixes payload with its 4-byte big-endian length.
func EncodeFrame(payload []byte) ([]byte, error) {
	if len(payload) > maxFrameLen {
		return nil, fmt.Errorf("frame too large: %d bytes", len(payload))
	}
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out[:4], uint32(len(payload)))
	copy(out[4:], payload)
	return out, nil
}
