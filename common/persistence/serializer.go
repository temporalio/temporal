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

	"github.com/gogo/protobuf/proto"
	commonproto "go.temporal.io/temporal-proto/common"

	"github.com/temporalio/temporal/common/persistence/serialization"

	persist "github.com/temporalio/temporal/.gen/go/persistenceblobs"
	workflow "github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/codec"
)

type (
	// PayloadSerializer is used by persistence to serialize/deserialize history event(s) and others
	// It will only be used inside persistence, so that serialize/deserialize is transparent for application
	PayloadSerializer interface {
		// serialize/deserialize history events
		SerializeBatchEvents(batch []*commonproto.HistoryEvent, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeBatchEvents(data *serialization.DataBlob) ([]*commonproto.HistoryEvent, error)

		// serialize/deserialize a single history event
		SerializeEvent(event *commonproto.HistoryEvent, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeEvent(data *serialization.DataBlob) (*commonproto.HistoryEvent, error)

		// serialize/deserialize visibility memo fields
		SerializeVisibilityMemo(memo *workflow.Memo, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeVisibilityMemo(data *serialization.DataBlob) (*workflow.Memo, error)

		// serialize/deserialize reset points
		SerializeResetPoints(event *workflow.ResetPoints, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeResetPoints(data *serialization.DataBlob) (*workflow.ResetPoints, error)

		// serialize/deserialize bad binaries
		SerializeBadBinaries(event *workflow.BadBinaries, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeBadBinaries(data *serialization.DataBlob) (*workflow.BadBinaries, error)

		// serialize/deserialize version histories
		SerializeVersionHistories(histories *workflow.VersionHistories, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeVersionHistories(data *serialization.DataBlob) (*workflow.VersionHistories, error)

		// serialize/deserialize immutable cluster metadata
		SerializeImmutableClusterMetadata(icm *persist.ImmutableClusterMetadata, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeImmutableClusterMetadata(data *serialization.DataBlob) (*persist.ImmutableClusterMetadata, error)
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

func (t *serializerImpl) SerializeBatchEvents(events []*commonproto.HistoryEvent, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	return t.serialize(&commonproto.History{Events: events}, encodingType)
}

func (t *serializerImpl) DeserializeBatchEvents(data *serialization.DataBlob) ([]*commonproto.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	events := &commonproto.History{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, events)
	case common.EncodingTypeProto3, common.EncodingTypeThriftRW:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, events)
	default:
		return nil, NewCadenceDeserializationError("DeserializeEvent invalid encoding")
	}
	if err != nil {
		return nil, err
	}
	return events.Events, nil
}

func (t *serializerImpl) SerializeEvent(event *commonproto.HistoryEvent, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if event == nil {
		return nil, nil
	}
	return t.serialize(event, encodingType)
}

func (t *serializerImpl) DeserializeEvent(data *serialization.DataBlob) (*commonproto.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	event := &commonproto.HistoryEvent{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, event)
	case common.EncodingTypeProto3, common.EncodingTypeThriftRW:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, event)
	default:
		return nil, NewCadenceDeserializationError("DeserializeEvent invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return event, err
}

func (t *serializerImpl) SerializeResetPoints(rp *workflow.ResetPoints, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if rp == nil {
		rp = &workflow.ResetPoints{}
	}
	return t.serialize(rp, encodingType)
}

func (t *serializerImpl) DeserializeResetPoints(data *serialization.DataBlob) (*workflow.ResetPoints, error) {
	var rp workflow.ResetPoints
	err := t.deserialize(data, &rp)
	return &rp, err
}

func (t *serializerImpl) SerializeBadBinaries(bb *workflow.BadBinaries, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if bb == nil {
		bb = &workflow.BadBinaries{}
	}
	return t.serialize(bb, encodingType)
}

func (t *serializerImpl) DeserializeBadBinaries(data *serialization.DataBlob) (*workflow.BadBinaries, error) {
	var bb workflow.BadBinaries
	err := t.deserialize(data, &bb)
	return &bb, err
}

func (t *serializerImpl) SerializeVisibilityMemo(memo *workflow.Memo, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if memo == nil {
		// Return nil here to be consistent with Event
		// This check is not duplicate as check in following serialize
		return nil, nil
	}
	return t.serialize(memo, encodingType)
}

func (t *serializerImpl) DeserializeVisibilityMemo(data *serialization.DataBlob) (*workflow.Memo, error) {
	var memo workflow.Memo
	err := t.deserialize(data, &memo)
	return &memo, err
}

func (t *serializerImpl) SerializeVersionHistories(histories *workflow.VersionHistories, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if histories == nil {
		return nil, nil
	}
	return t.serialize(histories, encodingType)
}

func (t *serializerImpl) DeserializeVersionHistories(data *serialization.DataBlob) (*workflow.VersionHistories, error) {
	var histories workflow.VersionHistories
	err := t.deserialize(data, &histories)
	return &histories, err
}

func (t *serializerImpl) SerializeImmutableClusterMetadata(icm *persist.ImmutableClusterMetadata, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if icm == nil {
		icm = &persist.ImmutableClusterMetadata{}
	}
	return t.serialize(icm, encodingType)
}

func (t *serializerImpl) DeserializeImmutableClusterMetadata(data *serialization.DataBlob) (*persist.ImmutableClusterMetadata, error) {
	var icm persist.ImmutableClusterMetadata
	err := t.deserialize(data, &icm)
	return &icm, err
}

func (t *serializerImpl) serializeProto(p serialization.ProtoMarshal, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if p == nil {
		return nil, nil
	}

	var data []byte
	var err error

	switch encodingType {
	case common.EncodingTypeProto3, common.EncodingTypeThriftRW:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		data, err = p.Marshal()
	case common.EncodingTypeJSON, common.EncodingTypeUnknown, common.EncodingTypeEmpty: // For backward-compatibility
		encodingType = common.EncodingTypeJSON
		pb, ok := p.(proto.Message)
		if !ok {
			return nil, NewCadenceSerializationError("could not cast protomarshal interface to proto.message")
		}
		data, err = codec.NewJSONPBEncoder().Encode(pb)
	default:
		return nil, NewUnknownEncodingTypeError(encodingType)
	}

	if err != nil {
		return nil, NewCadenceSerializationError(err.Error())
	}

	// Shouldn't happen, but keeping
	if data == nil {
		return nil, nil
	}

	return &serialization.DataBlob{
		Data:     data,
		Encoding: encodingType,
	}, nil
}

func (t *serializerImpl) serialize(input interface{}, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if input == nil {
		return nil, nil
	}

	if p, ok := input.(serialization.ProtoMarshal); ok {
		return t.serializeProto(p, encodingType)
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
	case *workflow.Memo:
		return t.thriftrwEncoder.Encode(input.(*workflow.Memo))
	case *workflow.ResetPoints:
		return t.thriftrwEncoder.Encode(input.(*workflow.ResetPoints))
	case *workflow.BadBinaries:
		return t.thriftrwEncoder.Encode(input.(*workflow.BadBinaries))
	case *workflow.VersionHistories:
		return t.thriftrwEncoder.Encode(input.(*workflow.VersionHistories))
	case *persist.ImmutableClusterMetadata:
		return t.thriftrwEncoder.Encode(input.(*persist.ImmutableClusterMetadata))
	default:
		return nil, nil
	}
}

func (t *serializerImpl) deserialize(data *serialization.DataBlob, target interface{}) error {
	if data == nil {
		return nil
	}
	if len(data.Data) == 0 {
		return NewCadenceDeserializationError("DeserializeEvent empty data")
	}
	var err error

	switch data.GetEncoding() {
	case common.EncodingTypeProto3:
		return NewCadenceDeserializationError(fmt.Sprintf("proto requires proto specific deserialization"))
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
	switch target := target.(type) {
	case *workflow.Memo:
		return t.thriftrwEncoder.Decode(data, target)
	case *workflow.ResetPoints:
		return t.thriftrwEncoder.Decode(data, target)
	case *workflow.BadBinaries:
		return t.thriftrwEncoder.Decode(data, target)
	case *workflow.VersionHistories:
		return t.thriftrwEncoder.Decode(data, target)
	case *persist.ImmutableClusterMetadata:
		return t.thriftrwEncoder.Decode(data, target)
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
