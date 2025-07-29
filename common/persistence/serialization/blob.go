package serialization

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
)

func HistoryBranchToBlob(info *persistencespb.HistoryBranch) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func HistoryBranchFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryBranch, error) {
	result := &persistencespb.HistoryBranch{}
	return result, ProtoDecode(data, result)
}

func WorkflowExecutionStateToBlob(info *persistencespb.WorkflowExecutionState) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func WorkflowExecutionStateFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error) {
	result := &persistencespb.WorkflowExecutionState{}
	if err := ProtoDecode(data, result); err != nil {
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
	return ProtoEncode(info)
}

func TransferTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TransferTaskInfo, error) {
	result := &persistencespb.TransferTaskInfo{}
	return result, ProtoDecode(data, result)
}

func TimerTaskInfoToBlob(info *persistencespb.TimerTaskInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func TimerTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerTaskInfo, error) {
	result := &persistencespb.TimerTaskInfo{}
	return result, ProtoDecode(data, result)
}

func ReplicationTaskInfoToBlob(info *persistencespb.ReplicationTaskInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func ReplicationTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ReplicationTaskInfo, error) {
	result := &persistencespb.ReplicationTaskInfo{}
	return result, ProtoDecode(data, result)
}

func VisibilityTaskInfoToBlob(info *persistencespb.VisibilityTaskInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func VisibilityTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.VisibilityTaskInfo, error) {
	result := &persistencespb.VisibilityTaskInfo{}
	return result, ProtoDecode(data, result)
}

func ArchivalTaskInfoToBlob(info *persistencespb.ArchivalTaskInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func ArchivalTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ArchivalTaskInfo, error) {
	result := &persistencespb.ArchivalTaskInfo{}
	return result, ProtoDecode(data, result)
}

func OutboundTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.OutboundTaskInfo, error) {
	result := &persistencespb.OutboundTaskInfo{}
	return result, ProtoDecode(data, result)
}

func QueueMetadataToBlob(metadata *persistencespb.QueueMetadata) (*commonpb.DataBlob, error) {
	// TODO change ENCODING_TYPE_JSON to ENCODING_TYPE_PROTO3
	return encodeBlob(metadata, enumspb.ENCODING_TYPE_JSON)
}

func QueueMetadataFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueMetadata, error) {
	result := &persistencespb.QueueMetadata{}
	return result, ProtoDecode(data, result)
}

func QueueStateToBlob(info *persistencespb.QueueState) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func QueueStateFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueState, error) {
	result := &persistencespb.QueueState{}
	return result, ProtoDecode(data, result)
}
