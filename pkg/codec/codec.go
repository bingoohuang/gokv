package codec

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// Codec encodes/decodes Go values to/from slices of bytes.
type Codec interface {
	// Marshal encodes a Go value to a slice of bytes.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal decodes a slice of bytes into a Go value.
	Unmarshal(data []byte, v interface{}) error
}

// Convenience variables
var (
	// JSON is a JSONcodec that encodes/decodes Go values to/from JSON.
	JSON = JSONcodec{}
	// Gob is a GobCodec that encodes/decodes Go values to/from gob.
	Gob = GobCodec{}
)

// JSONcodec encodes/decodes Go values to/from JSON.
// You can use encoding.JSON instead of creating an instance of this struct.
type JSONcodec struct{}

// Marshal encodes a Go value to JSON.
func (c JSONcodec) Marshal(v interface{}) ([]byte, error) { return json.Marshal(v) }

// Unmarshal decodes a JSON value into a Go value.
func (c JSONcodec) Unmarshal(data []byte, v interface{}) error { return json.Unmarshal(data, v) }

// GobCodec encodes/decodes Go values to/from gob.
// You can use encoding.Gob instead of creating an instance of this struct.
type GobCodec struct{}

// Marshal encodes a Go value to gob.
func (c GobCodec) Marshal(v interface{}) ([]byte, error) {
	buffer := new(bytes.Buffer)
	if err := gob.NewEncoder(buffer).Encode(v); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// Unmarshal decodes a gob value into a Go value.
func (c GobCodec) Unmarshal(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}
