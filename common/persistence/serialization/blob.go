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

package serialization

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/utf8validator"
)

func HistoryBranchToBlob(info *persistencespb.HistoryBranch) (*commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func HistoryBranchFromBlob(blob []byte, encoding string) (*persistencespb.HistoryBranch, error) {
	result := &persistencespb.HistoryBranch{}
	return result, proto3Decode(blob, encoding, result)
}

func WorkflowExecutionInfoFromBlob(blob []byte, encoding string) (*persistencespb.WorkflowExecutionInfo, error) {
	result := &persistencespb.WorkflowExecutionInfo{}
	return result, proto3Decode(blob, encoding, result)
}

func WorkflowExecutionStateToBlob(info *persistencespb.WorkflowExecutionState) (*commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func WorkflowExecutionStateFromBlob(blob []byte, encoding string) (*persistencespb.WorkflowExecutionState, error) {
	result := &persistencespb.WorkflowExecutionState{}
	return result, proto3Decode(blob, encoding, result)
}

func TransferTaskInfoToBlob(info *persistencespb.TransferTaskInfo) (*commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TransferTaskInfoFromBlob(blob []byte, encoding string) (*persistencespb.TransferTaskInfo, error) {
	result := &persistencespb.TransferTaskInfo{}
	return result, proto3Decode(blob, encoding, result)
}

func TimerTaskInfoToBlob(info *persistencespb.TimerTaskInfo) (*commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TimerTaskInfoFromBlob(blob []byte, encoding string) (*persistencespb.TimerTaskInfo, error) {
	result := &persistencespb.TimerTaskInfo{}
	return result, proto3Decode(blob, encoding, result)
}

func ReplicationTaskInfoToBlob(info *persistencespb.ReplicationTaskInfo) (*commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ReplicationTaskInfoFromBlob(blob []byte, encoding string) (*persistencespb.ReplicationTaskInfo, error) {
	result := &persistencespb.ReplicationTaskInfo{}
	return result, proto3Decode(blob, encoding, result)
}

func VisibilityTaskInfoToBlob(info *persistencespb.VisibilityTaskInfo) (*commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func VisibilityTaskInfoFromBlob(blob []byte, encoding string) (*persistencespb.VisibilityTaskInfo, error) {
	result := &persistencespb.VisibilityTaskInfo{}
	return result, proto3Decode(blob, encoding, result)
}

func ArchivalTaskInfoToBlob(info *persistencespb.ArchivalTaskInfo) (*commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ArchivalTaskInfoFromBlob(blob []byte, encoding string) (*persistencespb.ArchivalTaskInfo, error) {
	result := &persistencespb.ArchivalTaskInfo{}
	return result, proto3Decode(blob, encoding, result)
}

func OutboundTaskInfoFromBlob(blob []byte, encoding string) (*persistencespb.OutboundTaskInfo, error) {
	result := &persistencespb.OutboundTaskInfo{}
	return result, proto3Decode(blob, encoding, result)
}

func QueueMetadataToBlob(metadata *persistencespb.QueueMetadata) (*commonpb.DataBlob, error) {
	// TODO change ENCODING_TYPE_JSON to ENCODING_TYPE_PROTO3
	return encode(metadata, enumspb.ENCODING_TYPE_JSON)
}

func QueueMetadataFromBlob(blob []byte, encoding string) (*persistencespb.QueueMetadata, error) {
	result := &persistencespb.QueueMetadata{}
	return result, decode(blob, encoding, result)
}

func QueueStateToBlob(info *persistencespb.QueueState) (*commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func QueueStateFromBlob(blob []byte, encoding string) (*persistencespb.QueueState, error) {
	result := &persistencespb.QueueState{}
	return result, proto3Decode(blob, encoding, result)
}

func encode(
	object proto.Message,
	encoding enumspb.EncodingType,
) (*commonpb.DataBlob, error) {
	if object == nil {
		return &commonpb.DataBlob{
			Data:         nil,
			EncodingType: encoding,
		}, nil
	}

	switch encoding {
	case enumspb.ENCODING_TYPE_JSON:
		blob, err := codec.NewJSONPBEncoder().Encode(object)
		if err != nil {
			return nil, err
		}
		return &commonpb.DataBlob{
			Data:         blob,
			EncodingType: enumspb.ENCODING_TYPE_JSON,
		}, nil
	case enumspb.ENCODING_TYPE_PROTO3:
		return proto3Encode(object)
	default:
		return nil, NewUnknownEncodingTypeError(encoding.String(), enumspb.ENCODING_TYPE_JSON, enumspb.ENCODING_TYPE_PROTO3)
	}
}

func decode(
	blob []byte,
	encoding string,
	result proto.Message,
) error {
	if blob == nil {
		return nil
	}

	enc, _ := enumspb.EncodingTypeFromString(encoding)
	switch enc {
	case enumspb.ENCODING_TYPE_JSON:
		return codec.NewJSONPBEncoder().Decode(blob, result)
	case enumspb.ENCODING_TYPE_PROTO3:
		return proto3Decode(blob, encoding, result)
	default:
		return NewUnknownEncodingTypeError(encoding, enumspb.ENCODING_TYPE_JSON, enumspb.ENCODING_TYPE_PROTO3)
	}
}

func proto3Encode(m proto.Message) (*commonpb.DataBlob, error) {
	if err := utf8validator.Validate(m, utf8validator.SourcePersistence); err != nil {
		return nil, NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	blob := commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	blob.Data = data
	return &blob, nil
}

func proto3Decode(blob []byte, encoding string, result proto.Message) error {
	e, _ := enumspb.EncodingTypeFromString(encoding)
	if e != enumspb.ENCODING_TYPE_PROTO3 {
		return NewUnknownEncodingTypeError(encoding, enumspb.ENCODING_TYPE_PROTO3)
	}
	return Proto3Decode(blob, e, result)
}

func Proto3Decode(blob []byte, e enumspb.EncodingType, result proto.Message) error {
	if e != enumspb.ENCODING_TYPE_PROTO3 {
		return NewUnknownEncodingTypeError(e.String(), enumspb.ENCODING_TYPE_PROTO3)
	}
	err := proto.Unmarshal(blob, result)
	if err == nil {
		err = utf8validator.Validate(result, utf8validator.SourcePersistence)
	}
	if err != nil {
		return NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return nil
}
