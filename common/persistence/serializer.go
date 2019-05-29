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
	// PayloadSerializer is used by persistence to serialize/deserialize history event(s) and others
	// It will only be used inside persistence, so that serialize/deserialize is transparent for application
	PayloadSerializer interface {
		// serialize/deserialize history events
		SerializeBatchEvents(batch []*workflow.HistoryEvent, encodingType common.EncodingType) (*DataBlob, error)
		DeserializeBatchEvents(data *DataBlob) ([]*workflow.HistoryEvent, error)

		// serialize/deserialize a single history event
		SerializeEvent(event *workflow.HistoryEvent, encodingType common.EncodingType) (*DataBlob, error)
		DeserializeEvent(data *DataBlob) (*workflow.HistoryEvent, error)

		// serialize/deserialize visibility memo fields
		SerializeVisibilityMemo(memo *workflow.Memo, encodingType common.EncodingType) (*DataBlob, error)
		DeserializeVisibilityMemo(data *DataBlob) (*workflow.Memo, error)

		// serialize/deserialize reset points
		SerializeResetPoints(event *workflow.ResetPoints, encodingType common.EncodingType) (*DataBlob, error)
		DeserializeResetPoints(data *DataBlob) (*workflow.ResetPoints, error)

		// serialize/deserialize bad binaries
		SerializeBadBinaries(event *workflow.BadBinaries, encodingType common.EncodingType) (*DataBlob, error)
		DeserializeBadBinaries(data *DataBlob) (*workflow.BadBinaries, error)

		// serialize/deserialize version histories
		SerializeVersionHistories(histories *workflow.VersionHistories, encodingType common.EncodingType) (*DataBlob, error)
		DeserializeVersionHistories(data *DataBlob) (*workflow.VersionHistories, error)
	}

	// CadenceSerializationError is an error type for cadence serialization
	CadenceSerializationError struct {
		msg string
	}

	// CadenceDeserializationError is an error type for cadence deserialization
	CadenceDeserializationError struct {
		msg string
	}

	// UnknownEncodingTypeError is an error type for unknown or unsupported encoding type
	UnknownEncodingTypeError struct {
		encodingType common.EncodingType
	}

	serializerImpl struct {
		thriftrwEncoder codec.BinaryEncoder
	}
)

// NewPayloadSerializer returns a PayloadSerializer
func NewPayloadSerializer() PayloadSerializer {
	return &serializerImpl{
		thriftrwEncoder: codec.NewThriftRWEncoder(),
	}
}

func (t *serializerImpl) SerializeBatchEvents(events []*workflow.HistoryEvent, encodingType common.EncodingType) (*DataBlob, error) {
	return t.serialize(events, encodingType)
}

func (t *serializerImpl) DeserializeBatchEvents(data *DataBlob) ([]*workflow.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	var events []*workflow.HistoryEvent
	if data != nil && len(data.Data) == 0 {
		return events, nil
	}
	err := t.deserialize(data, &events)
	return events, err
}

func (t *serializerImpl) SerializeEvent(event *workflow.HistoryEvent, encodingType common.EncodingType) (*DataBlob, error) {
	if event == nil {
		return nil, nil
	}
	return t.serialize(event, encodingType)
}

func (t *serializerImpl) DeserializeEvent(data *DataBlob) (*workflow.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	var event workflow.HistoryEvent
	err := t.deserialize(data, &event)
	return &event, err
}

func (t *serializerImpl) SerializeResetPoints(rp *workflow.ResetPoints, encodingType common.EncodingType) (*DataBlob, error) {
	if rp == nil {
		rp = &workflow.ResetPoints{}
	}
	return t.serialize(rp, encodingType)
}

func (t *serializerImpl) DeserializeResetPoints(data *DataBlob) (*workflow.ResetPoints, error) {
	var rp workflow.ResetPoints
	err := t.deserialize(data, &rp)
	return &rp, err
}

func (t *serializerImpl) SerializeBadBinaries(bb *workflow.BadBinaries, encodingType common.EncodingType) (*DataBlob, error) {
	if bb == nil {
		bb = &workflow.BadBinaries{}
	}
	return t.serialize(bb, encodingType)
}

func (t *serializerImpl) DeserializeBadBinaries(data *DataBlob) (*workflow.BadBinaries, error) {
	var bb workflow.BadBinaries
	err := t.deserialize(data, &bb)
	return &bb, err
}

func (t *serializerImpl) SerializeVisibilityMemo(memo *workflow.Memo, encodingType common.EncodingType) (*DataBlob, error) {
	if memo == nil {
		// Return nil here to be consistent with Event
		// This check is not duplicate as check in following serialize
		return nil, nil
	}
	return t.serialize(memo, encodingType)
}

func (t *serializerImpl) DeserializeVisibilityMemo(data *DataBlob) (*workflow.Memo, error) {
	var memo workflow.Memo
	err := t.deserialize(data, &memo)
	return &memo, err
}

func (t *serializerImpl) SerializeVersionHistories(histories *workflow.VersionHistories, encodingType common.EncodingType) (*DataBlob, error) {
	if histories == nil {
		return nil, nil
	}
	return t.serialize(histories, encodingType)
}

func (t *serializerImpl) DeserializeVersionHistories(data *DataBlob) (*workflow.VersionHistories, error) {
	var histories workflow.VersionHistories
	err := t.deserialize(data, &histories)
	return &histories, err
}

func (t *serializerImpl) serialize(input interface{}, encodingType common.EncodingType) (*DataBlob, error) {
	if input == nil {
		return nil, nil
	}

	var data []byte
	var err error

	switch encodingType {
	case common.EncodingTypeThriftRW:
		data, err = t.thriftrwEncode(input)
	case common.EncodingTypeJSON, common.EncodingTypeUnknown, common.EncodingTypeEmpty: // For backward-compatibility
		encodingType = common.EncodingTypeJSON
		data, err = json.Marshal(input)
	default:
		return nil, NewUnknownEncodingTypeError(encodingType)
	}

	if err != nil {
		return nil, NewCadenceSerializationError(err.Error())
	}
	return NewDataBlob(data, encodingType), nil
}

func (t *serializerImpl) thriftrwEncode(input interface{}) ([]byte, error) {
	switch input.(type) {
	case []*workflow.HistoryEvent:
		return t.thriftrwEncoder.Encode(&workflow.History{Events: input.([]*workflow.HistoryEvent)})
	case *workflow.HistoryEvent:
		return t.thriftrwEncoder.Encode(input.(*workflow.HistoryEvent))
	case *workflow.Memo:
		return t.thriftrwEncoder.Encode(input.(*workflow.Memo))
	case *workflow.ResetPoints:
		return t.thriftrwEncoder.Encode(input.(*workflow.ResetPoints))
	case *workflow.BadBinaries:
		return t.thriftrwEncoder.Encode(input.(*workflow.BadBinaries))
	case *workflow.VersionHistories:
		return t.thriftrwEncoder.Encode(input.(*workflow.VersionHistories))
	default:
		return nil, nil
	}
}

func (t *serializerImpl) deserialize(data *DataBlob, target interface{}) error {
	if data == nil {
		return nil
	}
	if len(data.Data) == 0 {
		return NewCadenceDeserializationError("DeserializeEvent empty data")
	}
	var err error

	switch data.GetEncoding() {
	case common.EncodingTypeThriftRW:
		err = t.thriftrwDecode(data.Data, target)
	case common.EncodingTypeJSON, common.EncodingTypeUnknown, common.EncodingTypeEmpty: // For backward-compatibility
		err = json.Unmarshal(data.Data, target)
	default:
		return NewUnknownEncodingTypeError(data.GetEncoding())
	}

	if err != nil {
		return NewCadenceDeserializationError(fmt.Sprintf("DeserializeBatchEvents encoding: \"%v\", error: %v", data.Encoding, err.Error()))
	}
	return nil
}

func (t *serializerImpl) thriftrwDecode(data []byte, target interface{}) error {
	switch target.(type) {
	case *[]*workflow.HistoryEvent:
		history := workflow.History{Events: *target.(*[]*workflow.HistoryEvent)}
		if err := t.thriftrwEncoder.Decode(data, &history); err != nil {
			return err
		}
		*target.(*[]*workflow.HistoryEvent) = history.GetEvents()
		return nil
	case *workflow.HistoryEvent:
		event := target.(*workflow.HistoryEvent)
		return t.thriftrwEncoder.Decode(data, event)
	case *workflow.Memo:
		memo := target.(*workflow.Memo)
		t.thriftrwEncoder.Decode(data, memo)
		return nil
	case *workflow.ResetPoints:
		rp := target.(*workflow.ResetPoints)
		t.thriftrwEncoder.Decode(data, rp)
		return nil
	case *workflow.BadBinaries:
		rp := target.(*workflow.BadBinaries)
		t.thriftrwEncoder.Decode(data, rp)
		return nil
	case *workflow.VersionHistories:
		rp := target.(*workflow.VersionHistories)
		t.thriftrwEncoder.Decode(data, rp)
		return nil
	default:
		return nil
	}
}

// NewUnknownEncodingTypeError returns a new instance of encoding type error
func NewUnknownEncodingTypeError(encodingType common.EncodingType) error {
	return &UnknownEncodingTypeError{encodingType: encodingType}
}

func (e *UnknownEncodingTypeError) Error() string {
	return fmt.Sprintf("unknown or unsupported encoding type %v", e.encodingType)
}

// NewCadenceSerializationError returns a CadenceSerializationError
func NewCadenceSerializationError(msg string) *CadenceSerializationError {
	return &CadenceSerializationError{msg: msg}
}

func (e *CadenceSerializationError) Error() string {
	return fmt.Sprintf("cadence serialization error: %v", e.msg)
}

// NewCadenceDeserializationError returns a CadenceDeserializationError
func NewCadenceDeserializationError(msg string) *CadenceDeserializationError {
	return &CadenceDeserializationError{msg: msg}
}

func (e *CadenceDeserializationError) Error() string {
	return fmt.Sprintf("cadence deserialization error: %v", e.msg)
}
