package gob

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
)

var errEmptyArgument = errors.New("length of input argument is 0")

// Encoder is wrapper of gob encoder/decoder
type Encoder struct{}

// NewGobEncoder create new Encoder
func NewGobEncoder() *Encoder {
	return &Encoder{}
}

// Encode one or more objects to binary
func (gobEncoder *Encoder) Encode(value ...interface{}) ([]byte, error) {
	if len(value) == 0 {
		return nil, errEmptyArgument
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	for i, obj := range value {
		if err := enc.Encode(obj); err != nil {
			return nil, fmt.Errorf(
				"unable to encode argument: %d, %v, with gob error: %v", i, reflect.TypeOf(obj), err)
		}
	}
	return buf.Bytes(), nil
}

// Decode binary to one or more objects
func (gobEncoder *Encoder) Decode(input []byte, valuePtr ...interface{}) error {
	if len(valuePtr) == 0 {
		return errEmptyArgument
	}

	dec := gob.NewDecoder(bytes.NewBuffer(input))
	for i, obj := range valuePtr {
		if err := dec.Decode(obj); err != nil {
			return fmt.Errorf(
				"unable to decode argument: %d, %v, with gob error: %v", i, reflect.TypeOf(obj), err)
		}
	}
	return nil
}
