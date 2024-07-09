package hasher

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"hash"
)

const (
	BUFFER_INITITAL_SIZE = 1024
	NIL_VALUE            = "--NIL--"
)

type Hasher struct {
	buffer  *bytes.Buffer
	encoder *gob.Encoder
	state   hash.Hash
	count   int
}

func NewHasher() *Hasher {
	backingBuffer := make([]byte, BUFFER_INITITAL_SIZE)
	buffer := bytes.NewBuffer(backingBuffer)
	encoder := gob.NewEncoder(buffer)
	state := sha1.New()
	return &Hasher{
		buffer:  buffer,
		encoder: encoder,
		state:   state,
		count:   0,
	}
}

func (h *Hasher) Reset() {
	h.buffer.Reset()
	h.state.Reset()
	h.count = 0
}

func (h *Hasher) Update(data interface{}) {
	h.buffer.Reset()

	switch v := data.(type) {
	case int:
		h.encoder.Encode(data.(int))
	case string:
		h.encoder.Encode(data.(string))
	case float64:
		h.encoder.Encode(data.(float64))
	case bool:
		h.encoder.Encode(data.(bool))
	case nil:
		h.encoder.Encode(NIL_VALUE)
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
	// update the hash buffer with the encoded data
	h.state.Write(h.buffer.Bytes())
	h.count++
}

func (h *Hasher) GetHash() (uint64, error) {
	if h.count == 0 {
		return 0, fmt.Errorf("data is empty")
	}
	buffer := h.state.Sum(nil)
	// copy the last 8 bytes of the hash buffer into a uint64
	var hash uint64
	for i := 0; i < 8; i++ {
		hash <<= 8
		hash |= uint64(buffer[len(buffer)-8+i])
	}
	return hash, nil
}
