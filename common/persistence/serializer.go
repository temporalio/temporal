// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	commonpb "go.temporal.io/temporal-proto/common/v1"
	historypb "go.temporal.io/temporal-proto/history/v1"
	namespacepb "go.temporal.io/temporal-proto/namespace/v1"
	workflowpb "go.temporal.io/temporal-proto/workflow/v1"

	historyspb "github.com/temporalio/temporal/api/history/v1"
	"github.com/temporalio/temporal/api/persistenceblobs/v1"
	"github.com/temporalio/temporal/common/persistence/serialization"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/codec"
)

type (
	// PayloadSerializer is used by persistence to serialize/deserialize history event(s) and others
	// It will only be used inside persistence, so that serialize/deserialize is transparent for application
	PayloadSerializer interface {
		// serialize/deserialize history events
		SerializeBatchEvents(batch []*historypb.HistoryEvent, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeBatchEvents(data *serialization.DataBlob) ([]*historypb.HistoryEvent, error)

		// serialize/deserialize a single history event
		SerializeEvent(event *historypb.HistoryEvent, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeEvent(data *serialization.DataBlob) (*historypb.HistoryEvent, error)

		// serialize/deserialize visibility memo fields
		SerializeVisibilityMemo(memo *commonpb.Memo, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeVisibilityMemo(data *serialization.DataBlob) (*commonpb.Memo, error)

		// serialize/deserialize reset points
		SerializeResetPoints(event *workflowpb.ResetPoints, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeResetPoints(data *serialization.DataBlob) (*workflowpb.ResetPoints, error)

		// serialize/deserialize bad binaries
		SerializeBadBinaries(event *namespacepb.BadBinaries, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeBadBinaries(data *serialization.DataBlob) (*namespacepb.BadBinaries, error)

		// serialize/deserialize version histories
		SerializeVersionHistories(histories *historyspb.VersionHistories, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeVersionHistories(data *serialization.DataBlob) (*historyspb.VersionHistories, error)

		// serialize/deserialize immutable cluster metadata
		SerializeImmutableClusterMetadata(icm *persistenceblobs.ImmutableClusterMetadata, encodingType common.EncodingType) (*serialization.DataBlob, error)
		DeserializeImmutableClusterMetadata(data *serialization.DataBlob) (*persistenceblobs.ImmutableClusterMetadata, error)
	}

	// SerializationError is an error type for serialization
	SerializationError struct {
		msg string
	}

	// DeserializationError is an error type for deserialization
	DeserializationError struct {
		msg string
	}

	// UnknownEncodingTypeError is an error type for unknown or unsupported encoding type
	UnknownEncodingTypeError struct {
		encodingType common.EncodingType
	}

	serializerImpl struct{}
)

// NewPayloadSerializer returns a PayloadSerializer
func NewPayloadSerializer() PayloadSerializer {
	return &serializerImpl{}
}

func (t *serializerImpl) SerializeBatchEvents(events []*historypb.HistoryEvent, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	return t.serialize(&historypb.History{Events: events}, encodingType)
}

func (t *serializerImpl) DeserializeBatchEvents(data *serialization.DataBlob) ([]*historypb.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	events := &historypb.History{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, events)
	case common.EncodingTypeProto3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, events)
	default:
		return nil, NewDeserializationError("DeserializeBatchEvents invalid encoding")
	}
	if err != nil {
		return nil, err
	}
	return events.Events, nil
}

func (t *serializerImpl) SerializeEvent(event *historypb.HistoryEvent, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if event == nil {
		return nil, nil
	}
	return t.serialize(event, encodingType)
}

func (t *serializerImpl) DeserializeEvent(data *serialization.DataBlob) (*historypb.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	event := &historypb.HistoryEvent{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, event)
	case common.EncodingTypeProto3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, event)
	default:
		return nil, NewDeserializationError("DeserializeEvent invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return event, err
}

func (t *serializerImpl) SerializeResetPoints(rp *workflowpb.ResetPoints, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if rp == nil {
		rp = &workflowpb.ResetPoints{}
	}
	return t.serialize(rp, encodingType)
}

func (t *serializerImpl) DeserializeResetPoints(data *serialization.DataBlob) (*workflowpb.ResetPoints, error) {
	if data == nil {
		return &workflowpb.ResetPoints{}, nil
	}
	if len(data.Data) == 0 {
		return &workflowpb.ResetPoints{}, nil
	}

	memo := &workflowpb.ResetPoints{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, memo)
	case common.EncodingTypeProto3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, memo)
	default:
		return nil, NewDeserializationError("DeserializeResetPoints invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeBadBinaries(bb *namespacepb.BadBinaries, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if bb == nil {
		bb = &namespacepb.BadBinaries{}
	}
	return t.serialize(bb, encodingType)
}

func (t *serializerImpl) DeserializeBadBinaries(data *serialization.DataBlob) (*namespacepb.BadBinaries, error) {
	if data == nil {
		return &namespacepb.BadBinaries{}, nil
	}
	if len(data.Data) == 0 {
		return &namespacepb.BadBinaries{}, nil
	}

	memo := &namespacepb.BadBinaries{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, memo)
	case common.EncodingTypeProto3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, memo)
	default:
		return nil, NewDeserializationError("DeserializeBadBinaries invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeVisibilityMemo(memo *commonpb.Memo, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if memo == nil {
		// Return nil here to be consistent with Event
		// This check is not duplicate as check in following serialize
		return nil, nil
	}
	return t.serialize(memo, encodingType)
}

func (t *serializerImpl) DeserializeVisibilityMemo(data *serialization.DataBlob) (*commonpb.Memo, error) {
	if data == nil {
		return &commonpb.Memo{}, nil
	}
	if len(data.Data) == 0 {
		return &commonpb.Memo{}, nil
	}

	memo := &commonpb.Memo{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, memo)
	case common.EncodingTypeProto3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, memo)
	default:
		return nil, NewDeserializationError("DeserializeVisibilityMemo invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeVersionHistories(histories *historyspb.VersionHistories, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if histories == nil {
		return nil, nil
	}
	return t.serialize(histories, encodingType)
}

func (t *serializerImpl) DeserializeVersionHistories(data *serialization.DataBlob) (*historyspb.VersionHistories, error) {
	if data == nil {
		return &historyspb.VersionHistories{}, nil
	}
	if len(data.Data) == 0 {
		return &historyspb.VersionHistories{}, nil
	}

	memo := &historyspb.VersionHistories{}
	var err error
	switch data.Encoding {
	case common.EncodingTypeJSON:
		err = codec.NewJSONPBEncoder().Decode(data.Data, memo)
	case common.EncodingTypeProto3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, memo)
	default:
		return nil, NewDeserializationError("DeserializeVersionHistories invalid encoding")
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
	case common.EncodingTypeProto3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = proto.Unmarshal(data.Data, event)
	default:
		return nil, NewDeserializationError("DeserializeImmutableClusterMetadata invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return event, err
}

func (t *serializerImpl) serializeProto(p proto.Marshaler, encodingType common.EncodingType) (*serialization.DataBlob, error) {
	if p == nil {
		return nil, nil
	}

	var data []byte
	var err error

	switch encodingType {
	case common.EncodingTypeProto3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		data, err = p.Marshal()
	case common.EncodingTypeJSON, common.EncodingTypeUnknown, common.EncodingTypeEmpty: // For backward-compatibility
		encodingType = common.EncodingTypeJSON
		pb, ok := p.(proto.Message)
		if !ok {
			return nil, NewSerializationError("could not cast protomarshal interface to proto.message")
		}
		data, err = codec.NewJSONPBEncoder().Encode(pb)
	default:
		return nil, NewUnknownEncodingTypeError(encodingType)
	}

	if err != nil {
		return nil, NewSerializationError(err.Error())
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

	if p, ok := input.(proto.Marshaler); ok {
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
		return nil, NewSerializationError(err.Error())
	}

	return NewDataBlob(data, encodingType), nil
}

func (t *serializerImpl) deserialize(data *serialization.DataBlob, target interface{}) error {
	if data == nil {
		return nil
	}
	if len(data.Data) == 0 {
		return NewDeserializationError("DeserializeEvent empty data")
	}
	var err error

	switch data.GetEncoding() {
	case common.EncodingTypeProto3:
		return NewDeserializationError(fmt.Sprintf("proto requires proto specific deserialization"))
	case common.EncodingTypeJSON, common.EncodingTypeUnknown, common.EncodingTypeEmpty: // For backward-compatibility
		err = json.Unmarshal(data.Data, target)
	default:
		return NewUnknownEncodingTypeError(data.GetEncoding())
	}

	if err != nil {
		return NewDeserializationError(fmt.Sprintf("DeserializeBatchEvents encoding: \"%v\", error: %v", data.Encoding, err.Error()))
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

// NewSerializationError returns a SerializationError
func NewSerializationError(msg string) *SerializationError {
	return &SerializationError{msg: msg}
}

func (e *SerializationError) Error() string {
	return fmt.Sprintf("cadence serialization error: %v", e.msg)
}

// NewDeserializationError returns a DeserializationError
func NewDeserializationError(msg string) *DeserializationError {
	return &DeserializationError{msg: msg}
}

func (e *DeserializationError) Error() string {
	return fmt.Sprintf("cadence deserialization error: %v", e.msg)
}
