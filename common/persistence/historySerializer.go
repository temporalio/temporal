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

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
)

type (
	// HistorySerializer is used by persistence to serialize/deserialize history event(s)
	// It will only be used inside persistence, so that serialize/deserialize is transparent for application
	HistorySerializer interface {
		// serialize/deserialize history events
		SerializeBatchEvents(batch []*workflow.HistoryEvent, encodingType common.EncodingType) (*DataBlob, error)
		DeserializeBatchEvents(data *DataBlob) ([]*workflow.HistoryEvent, error)

		// serialize/deserialize a single history event
		SerializeEvent(event *workflow.HistoryEvent, encodingType common.EncodingType) (*DataBlob, error)
		DeserializeEvent(data *DataBlob) (*workflow.HistoryEvent, error)
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

	serializerImpl struct {
		thriftrwEncoder codec.BinaryEncoder
	}
)

// NewHistorySerializer returns a HistorySerializer
func NewHistorySerializer() HistorySerializer {
	return &serializerImpl{
		thriftrwEncoder: codec.NewThriftRWEncoder(),
	}
}

func (t *serializerImpl) SerializeBatchEvents(events []*workflow.HistoryEvent, encodingType common.EncodingType) (*DataBlob, error) {
	batch := &workflow.History{Events: events}
	if batch == nil {
		batch = &workflow.History{}
	}

	switch encodingType {
	case common.EncodingTypeGob:
		return nil, NewUnknownEncodingTypeError(encodingType)
	case common.EncodingTypeThriftRW:
		history := &workflow.History{
			Events: batch.Events,
		}
		data, err := t.thriftrwEncoder.Encode(history)
		if err != nil {
			return nil, &HistorySerializationError{msg: err.Error()}
		}
		return NewDataBlob(data, encodingType), nil
	default:
		fallthrough
	case common.EncodingTypeJSON:
		data, err := json.Marshal(batch.Events)
		if err != nil {
			return nil, &HistorySerializationError{msg: err.Error()}
		}
		return NewDataBlob(data, common.EncodingTypeJSON), nil
	}
}

func (t *serializerImpl) DeserializeBatchEvents(data *DataBlob) ([]*workflow.HistoryEvent, error) {
	switch data.GetEncoding() {
	//As backward-compatibility, unknown should be json
	case common.EncodingTypeUnknown:
		fallthrough
	case common.EncodingTypeJSON:
		var events []*workflow.HistoryEvent
		if len(data.Data) == 0 {
			return events, nil
		}
		err := json.Unmarshal(data.Data, &events)
		if err != nil {
			return nil, &HistoryDeserializationError{msg: err.Error()}
		}
		return events, nil
	case common.EncodingTypeThriftRW:
		var history workflow.History
		err := t.thriftrwEncoder.Decode(data.Data, &history)
		if err != nil {
			return nil, &HistoryDeserializationError{msg: err.Error()}
		}
		return history.Events, nil
	default:
		return nil, NewUnknownEncodingTypeError(data.GetEncoding())
	}
}

func (t *serializerImpl) SerializeEvent(event *workflow.HistoryEvent, encodingType common.EncodingType) (*DataBlob, error) {
	if event == nil {
		event = &workflow.HistoryEvent{}
	}
	switch encodingType {
	case common.EncodingTypeGob:
		return nil, NewUnknownEncodingTypeError(encodingType)
	case common.EncodingTypeThriftRW:
		data, err := t.thriftrwEncoder.Encode(event)
		if err != nil {
			return nil, &HistorySerializationError{msg: err.Error()}
		}
		return NewDataBlob(data, encodingType), nil
	default:
		fallthrough
	case common.EncodingTypeJSON:
		data, err := json.Marshal(event)
		if err != nil {
			return nil, &HistorySerializationError{msg: err.Error()}
		}
		return NewDataBlob(data, common.EncodingTypeJSON), nil
	}
}

func (t *serializerImpl) DeserializeEvent(data *DataBlob) (*workflow.HistoryEvent, error) {
	var event workflow.HistoryEvent
	switch data.GetEncoding() {
	//As backward-compatibility, unknown should be json
	case common.EncodingTypeUnknown:
		fallthrough
	case common.EncodingTypeJSON:
		if len(data.Data) == 0 {
			return &event, nil
		}
		err := json.Unmarshal(data.Data, &event)
		if err != nil {
			return nil, &HistoryDeserializationError{msg: err.Error()}
		}
		return &event, nil
	case common.EncodingTypeThriftRW:
		err := t.thriftrwEncoder.Decode(data.Data, &event)
		if err != nil {
			return nil, &HistoryDeserializationError{msg: err.Error()}
		}
		return &event, nil
	default:
		return nil, NewUnknownEncodingTypeError(data.GetEncoding())
	}
}

// NewUnknownEncodingTypeError returns a new instance of encoding type error
func NewUnknownEncodingTypeError(encodingType common.EncodingType) error {
	return &UnknownEncodingTypeError{encodingType: encodingType}
}

func (e *UnknownEncodingTypeError) Error() string {
	return fmt.Sprintf("unknown or unsupported encoding type %v", e.encodingType)
}

//NewHistorySerializationError returns a HistorySerializationError
func NewHistorySerializationError(msg string) *HistorySerializationError {
	return &HistorySerializationError{msg: msg}
}

func (e *HistorySerializationError) Error() string {
	return fmt.Sprintf("history serialization error: %v", e.msg)
}

func (e *HistoryDeserializationError) Error() string {
	return fmt.Sprintf("history deserialization error: %v", e.msg)
}
