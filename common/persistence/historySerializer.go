// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package persistence

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type (
	// HistorySerializer is used to serialize/deserialize history
	HistorySerializer interface {
		Serialize(batch *HistoryEventBatch) (*SerializedHistoryEventBatch, error)
		Deserialize(batch *SerializedHistoryEventBatch) (*HistoryEventBatch, error)
	}

	// HistorySerializerFactory is a factory that vends
	// HistorySerializers based on encoding type.
	HistorySerializerFactory interface {
		// Get returns a history serializer corresponding
		// to a given encoding type
		Get(encodingType common.EncodingType) (HistorySerializer, error)
	}

	// HistorySerializationError is an error type that's
	// returned on a history serialization failure
	HistorySerializationError struct {
		msg string
	}

	// HistoryDeserializationError is an error type that's
	// returned on a history deserialization failure
	HistoryDeserializationError struct {
		msg string
	}

	// UnknownEncodingTypeError is an error type that's
	// returned when the encoding type provided as input
	// is unknown or unsupported
	UnknownEncodingTypeError struct {
		encodingType common.EncodingType
	}

	// HistoryVersionCompatibilityError is an error type
	// that's returned when history serialization or
	// deserialization cannot proceed due to version
	// incompatibility
	HistoryVersionCompatibilityError struct {
		requiredVersion  int
		supportedVersion int
	}

	jsonHistorySerializer struct{}

	serializerFactoryImpl struct {
		jsonSerializer HistorySerializer
	}
)

const (
	// DefaultEncodingType is the default encoding format for persisted history
	DefaultEncodingType = common.EncodingTypeJSON
)

var defaultHistoryVersion = int32(1)
var maxSupportedHistoryVersion = int32(1)

// NewJSONHistorySerializer returns a JSON HistorySerializer
func NewJSONHistorySerializer() HistorySerializer {
	return &jsonHistorySerializer{}
}

func (j *jsonHistorySerializer) Serialize(batch *HistoryEventBatch) (*SerializedHistoryEventBatch, error) {

	if batch.Version > GetMaxSupportedHistoryVersion() {
		err := NewHistoryVersionCompatibilityError(batch.Version, GetMaxSupportedHistoryVersion())
		return nil, &HistorySerializationError{msg: err.Error()}
	}

	data, err := json.Marshal(batch.Events)
	if err != nil {
		return nil, &HistorySerializationError{msg: err.Error()}
	}
	return NewSerializedHistoryEventBatch(data, common.EncodingTypeJSON, batch.Version), nil
}

func (j *jsonHistorySerializer) Deserialize(batch *SerializedHistoryEventBatch) (*HistoryEventBatch, error) {

	if batch.Version > GetMaxSupportedHistoryVersion() {
		err := NewHistoryVersionCompatibilityError(batch.Version, GetMaxSupportedHistoryVersion())
		return nil, &HistoryDeserializationError{msg: err.Error()}
	}

	var events []*workflow.HistoryEvent
	err := json.Unmarshal(batch.Data, &events)
	if err != nil {
		return nil, &HistoryDeserializationError{msg: err.Error()}
	}
	return &HistoryEventBatch{Version: batch.Version, Events: events}, nil
}

// NewHistorySerializerFactory creates and returns an instance
// of HistorySerializerFactory
func NewHistorySerializerFactory() HistorySerializerFactory {
	return &serializerFactoryImpl{
		jsonSerializer: NewJSONHistorySerializer(),
	}
}

// Get returns the serializer corresponding to the given encoding type
func (f *serializerFactoryImpl) Get(encodingType common.EncodingType) (HistorySerializer, error) {
	switch encodingType {
	case common.EncodingTypeJSON:
		return f.jsonSerializer, nil
	default:
		return nil, NewUnknownEncodingTypeError(encodingType)
	}
}

// NewUnknownEncodingTypeError returns a new instance of encoding type error
func NewUnknownEncodingTypeError(encodingType common.EncodingType) error {
	return &UnknownEncodingTypeError{encodingType: encodingType}
}

func (e *UnknownEncodingTypeError) Error() string {
	return fmt.Sprintf("unknown or unsupported encoding type %v", e.encodingType)
}

// NewHistoryVersionCompatibilityError returns a new instance of compatibility error type
func NewHistoryVersionCompatibilityError(required int, supported int) error {
	return &HistoryVersionCompatibilityError{
		requiredVersion:  required,
		supportedVersion: supported,
	}
}

func (e *HistoryVersionCompatibilityError) Error() string {
	return fmt.Sprintf("incompatible history version;required=%v;maxSupported=%v",
		e.requiredVersion, e.supportedVersion)
}

func (e *HistorySerializationError) Error() string {
	return fmt.Sprintf("history serialization error: %v", e.msg)
}

func (e *HistoryDeserializationError) Error() string {
	return fmt.Sprintf("history deserialization error: %v", e.msg)
}

// SetMaxSupportedHistoryVersion resets the max supported history version
// this method is only intended for integration test
func SetMaxSupportedHistoryVersion(version int) {
	atomic.StoreInt32(&maxSupportedHistoryVersion, int32(version))
}

// GetMaxSupportedHistoryVersion returns the max supported version
func GetMaxSupportedHistoryVersion() int {
	return int(atomic.LoadInt32(&maxSupportedHistoryVersion))
}

// SetDefaultHistoryVersion resets the default history version
// only intended for integration test
func SetDefaultHistoryVersion(version int) {
	atomic.StoreInt32(&defaultHistoryVersion, int32(version))
}

// GetDefaultHistoryVersion returns the default history version
func GetDefaultHistoryVersion() int {
	return int(atomic.LoadInt32(&defaultHistoryVersion))
}
