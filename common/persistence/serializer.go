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

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/persistence/serialization"

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
		SerializeVisibilityMemo(memo *commonproto.Memo, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeVisibilityMemo(data *serialization.DataBlob) (*commonproto.Memo, error)

		// serialize/deserialize reset points
		SerializeResetPoints(event *commonproto.ResetPoints, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeResetPoints(data *serialization.DataBlob) (*commonproto.ResetPoints, error)

		// serialize/deserialize bad binaries
		SerializeBadBinaries(event *commonproto.BadBinaries, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeBadBinaries(data *serialization.DataBlob) (*commonproto.BadBinaries, error)

		// serialize/deserialize version histories
		SerializeVersionHistories(histories *commonproto.VersionHistories, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeVersionHistories(data *serialization.DataBlob) (*commonproto.VersionHistories, error)

		// serialize/deserialize immutable cluster metadata
		SerializeImmutableClusterMetadata(icm *persistenceblobs.ImmutableClusterMetadata, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeImmutableClusterMetadata(data *serialization.DataBlob) (*persistenceblobs.ImmutableClusterMetadata, error)
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
		return nil, NewCadenceDeserializationError("DeserializeBatchEvents invalid encoding")
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

func (t *serializerImpl) SerializeResetPoints(rp *commonproto.ResetPoints, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if rp == nil {
		rp = &commonproto.ResetPoints{}
	}
	return t.serialize(rp, encodingType)
}

func (t *serializerImpl) DeserializeResetPoints(data *serialization.DataBlob) (*commonproto.ResetPoints, error) {
	if data == nil {
		return &commonproto.ResetPoints{}, nil
	}
	if len(data.Data) == 0 {
		return &commonproto.ResetPoints{}, nil
	}

	memo := &commonproto.ResetPoints{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, memo)
	case common.EncodingTypeProto3, common.EncodingTypeThriftRW:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, memo)
	default:
		return nil, NewCadenceDeserializationError("DeserializeResetPoints invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeBadBinaries(bb *commonproto.BadBinaries, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if bb == nil {
		bb = &commonproto.BadBinaries{}
	}
	return t.serialize(bb, encodingType)
}

func (t *serializerImpl) DeserializeBadBinaries(data *serialization.DataBlob) (*commonproto.BadBinaries, error) {
	if data == nil {
		return &commonproto.BadBinaries{}, nil
	}
	if len(data.Data) == 0 {
		return &commonproto.BadBinaries{}, nil
	}

	memo := &commonproto.BadBinaries{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, memo)
	case common.EncodingTypeProto3, common.EncodingTypeThriftRW:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, memo)
	default:
		return nil, NewCadenceDeserializationError("DeserializeBadBinaries invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeVisibilityMemo(memo *commonproto.Memo, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if memo == nil {
		// Return nil here to be consistent with Event
		// This check is not duplicate as check in following serialize
		return nil, nil
	}
	return t.serialize(memo, encodingType)
}

func (t *serializerImpl) DeserializeVisibilityMemo(data *serialization.DataBlob) (*commonproto.Memo, error) {
	if data == nil {
		return &commonproto.Memo{}, nil
	}
	if len(data.Data) == 0 {
		return &commonproto.Memo{}, nil
	}

	memo := &commonproto.Memo{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, memo)
	case common.EncodingTypeProto3, common.EncodingTypeThriftRW:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, memo)
	default:
		return nil, NewCadenceDeserializationError("DeserializeVisibilityMemo invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeVersionHistories(histories *commonproto.VersionHistories, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if histories == nil {
		return nil, nil
	}
	return t.serialize(histories, encodingType)
}

func (t *serializerImpl) DeserializeVersionHistories(data *serialization.DataBlob) (*commonproto.VersionHistories, error) {
	if data == nil {
		return &commonproto.VersionHistories{}, nil
	}
	if len(data.Data) == 0 {
		return &commonproto.VersionHistories{}, nil
	}

	memo := &commonproto.VersionHistories{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, memo)
	case common.EncodingTypeProto3, common.EncodingTypeThriftRW:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, memo)
	default:
		return nil, NewCadenceDeserializationError("DeserializeVersionHistories invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeImmutableClusterMetadata(icm *persistenceblobs.ImmutableClusterMetadata, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if icm == nil {
		icm = &persistenceblobs.ImmutableClusterMetadata{}
	}
	return t.serialize(icm, encodingType)
}

func (t *serializerImpl) DeserializeImmutableClusterMetadata(data *serialization.DataBlob) (*persistenceblobs.ImmutableClusterMetadata, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	event := &persistenceblobs.ImmutableClusterMetadata{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, event)
	case common.EncodingTypeProto3, common.EncodingTypeThriftRW:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, event)
	default:
		return nil, NewCadenceDeserializationError("DeserializeImmutableClusterMetadata invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return event, err
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
