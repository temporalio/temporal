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
	"fmt"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	// PayloadSerializer is used by persistence to serialize/deserialize history event(s) and others
	// It will only be used inside persistence, so that serialize/deserialize is transparent for application
	PayloadSerializer interface {
		// serialize/deserialize history events
		SerializeEvents(batch []*historypb.HistoryEvent, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		DeserializeEvents(data *commonpb.DataBlob) ([]*historypb.HistoryEvent, error)

		// serialize/deserialize a single history event
		SerializeEvent(event *historypb.HistoryEvent, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		DeserializeEvent(data *commonpb.DataBlob) (*historypb.HistoryEvent, error)

		// serialize/deserialize visibility memo fields
		SerializeVisibilityMemo(memo *commonpb.Memo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		DeserializeVisibilityMemo(data *commonpb.DataBlob) (*commonpb.Memo, error)

		// serialize/deserialize reset points
		SerializeResetPoints(event *workflowpb.ResetPoints, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		DeserializeResetPoints(data *commonpb.DataBlob) (*workflowpb.ResetPoints, error)

		// serialize/deserialize bad binaries
		SerializeBadBinaries(event *namespacepb.BadBinaries, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		DeserializeBadBinaries(data *commonpb.DataBlob) (*namespacepb.BadBinaries, error)

		// serialize/deserialize mutable cluster metadata
		SerializeClusterMetadata(icm *persistencespb.ClusterMetadata, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		DeserializeClusterMetadata(data *commonpb.DataBlob) (*persistencespb.ClusterMetadata, error)
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
		encodingType enumspb.EncodingType
	}

	serializerImpl struct{}
)

// NewPayloadSerializer returns a PayloadSerializer
func NewPayloadSerializer() PayloadSerializer {
	return &serializerImpl{}
}

func (t *serializerImpl) SerializeEvents(events []*historypb.HistoryEvent, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return t.serialize(&historypb.History{Events: events}, encodingType)
}

func (t *serializerImpl) DeserializeEvents(data *commonpb.DataBlob) ([]*historypb.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	events := &historypb.History{}
	var err error
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = events.Unmarshal(data.Data)
	default:
		return nil, NewDeserializationError("DeserializeEvents invalid encoding")
	}
	if err != nil {
		return nil, err
	}
	return events.Events, nil
}

func (t *serializerImpl) SerializeEvent(event *historypb.HistoryEvent, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if event == nil {
		return nil, nil
	}
	return t.serialize(event, encodingType)
}

func (t *serializerImpl) DeserializeEvent(data *commonpb.DataBlob) (*historypb.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	event := &historypb.HistoryEvent{}
	var err error
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = event.Unmarshal(data.Data)
	default:
		return nil, NewDeserializationError("DeserializeEvent invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return event, err
}

func (t *serializerImpl) SerializeResetPoints(rp *workflowpb.ResetPoints, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if rp == nil {
		rp = &workflowpb.ResetPoints{}
	}
	return t.serialize(rp, encodingType)
}

func (t *serializerImpl) DeserializeResetPoints(data *commonpb.DataBlob) (*workflowpb.ResetPoints, error) {
	if data == nil {
		return &workflowpb.ResetPoints{}, nil
	}
	if len(data.Data) == 0 {
		return &workflowpb.ResetPoints{}, nil
	}

	memo := &workflowpb.ResetPoints{}
	var err error
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = memo.Unmarshal(data.Data)
	default:
		return nil, NewDeserializationError("DeserializeResetPoints invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeBadBinaries(bb *namespacepb.BadBinaries, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if bb == nil {
		bb = &namespacepb.BadBinaries{}
	}
	return t.serialize(bb, encodingType)
}

func (t *serializerImpl) DeserializeBadBinaries(data *commonpb.DataBlob) (*namespacepb.BadBinaries, error) {
	if data == nil {
		return &namespacepb.BadBinaries{}, nil
	}
	if len(data.Data) == 0 {
		return &namespacepb.BadBinaries{}, nil
	}

	memo := &namespacepb.BadBinaries{}
	var err error
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = memo.Unmarshal(data.Data)
	default:
		return nil, NewDeserializationError("DeserializeBadBinaries invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeVisibilityMemo(memo *commonpb.Memo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if memo == nil {
		// Return nil here to be consistent with Event
		// This check is not duplicate as check in following serialize
		return nil, nil
	}
	return t.serialize(memo, encodingType)
}

func (t *serializerImpl) DeserializeVisibilityMemo(data *commonpb.DataBlob) (*commonpb.Memo, error) {
	if data == nil {
		return &commonpb.Memo{}, nil
	}
	if len(data.Data) == 0 {
		return &commonpb.Memo{}, nil
	}

	memo := &commonpb.Memo{}
	var err error
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = memo.Unmarshal(data.Data)
	default:
		return nil, NewDeserializationError("DeserializeVisibilityMemo invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return memo, err
}

func (t *serializerImpl) SerializeClusterMetadata(cm *persistencespb.ClusterMetadata, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if cm == nil {
		cm = &persistencespb.ClusterMetadata{}
	}
	return t.serialize(cm, encodingType)
}

func (t *serializerImpl) DeserializeClusterMetadata(data *commonpb.DataBlob) (*persistencespb.ClusterMetadata, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	cm := &persistencespb.ClusterMetadata{}
	var err error
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Thrift == Proto for this object so that we can maintain test behavior until thrift is gone
		// Client API currently specifies encodingType on requests which span multiple of these objects
		err = cm.Unmarshal(data.Data)
	default:
		return nil, NewDeserializationError("DeserializeClusterMetadata invalid encoding")
	}

	if err != nil {
		return nil, err
	}

	return cm, err
}

func (t *serializerImpl) serialize(p proto.Marshaler, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if p == nil {
		return nil, nil
	}

	var data []byte
	var err error

	switch encodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Client API currently specifies encodingType on requests which span multiple of these objects
		data, err = p.Marshal()
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

	return &commonpb.DataBlob{
		Data:         data,
		EncodingType: encodingType,
	}, nil
}

// NewUnknownEncodingTypeError returns a new instance of encoding type error
func NewUnknownEncodingTypeError(encodingType enumspb.EncodingType) error {
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
	return fmt.Sprintf("serialization error: %v", e.msg)
}

// NewDeserializationError returns a DeserializationError
func NewDeserializationError(msg string) *DeserializationError {
	return &DeserializationError{msg: msg}
}

func (e *DeserializationError) Error() string {
	return fmt.Sprintf("deserialization error: %v", e.msg)
}
