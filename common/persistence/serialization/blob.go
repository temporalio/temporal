package serialization

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/codec"
	"google.golang.org/protobuf/proto"
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
	if err := proto3Decode(blob, encoding, result); err != nil {
		return nil, err
	}
	// Initialize the WorkflowExecutionStateDetails for old records.
	if result.RequestIds == nil {
		result.RequestIds = make(map[string]*persistencespb.RequestIDInfo, 1)
	}
	if result.CreateRequestId != "" && result.RequestIds[result.CreateRequestId] == nil {
		result.RequestIds[result.CreateRequestId] = &persistencespb.RequestIDInfo{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			EventId:   common.FirstEventID,
		}
	}
	return result, nil
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
	return codec.EncodeBlob(metadata, enumspb.ENCODING_TYPE_JSON)
}

func QueueMetadataFromBlob(blob []byte, encoding string) (*persistencespb.QueueMetadata, error) {
	result := &persistencespb.QueueMetadata{}
	enc, _ := enumspb.EncodingTypeFromString(encoding)
	return result, codec.DecodeBlob(&commonpb.DataBlob{Data: blob, EncodingType: enc}, result)
}

func QueueStateToBlob(info *persistencespb.QueueState) (*commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func QueueStateFromBlob(blob []byte, encoding string) (*persistencespb.QueueState, error) {
	result := &persistencespb.QueueState{}
	return result, proto3Decode(blob, encoding, result)
}

func proto3Encode(m proto.Message) (*commonpb.DataBlob, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: data}, nil
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
	if err != nil {
		return NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return nil
}
