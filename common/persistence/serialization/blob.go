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
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func ShardInfoToBlob(info *persistencespb.ShardInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ShardInfoFromBlob(b []byte, proto string, clusterName string) (*persistencespb.ShardInfo, error) {
	shardInfo := &persistencespb.ShardInfo{}
	err := proto3Decode(b, proto, shardInfo)

	if err != nil {
		return nil, err
	}

	if len(shardInfo.GetClusterTransferAckLevel()) == 0 {
		shardInfo.ClusterTransferAckLevel = map[string]int64{
			clusterName: shardInfo.GetTransferAckLevel(),
		}
	}

	if len(shardInfo.GetClusterTimerAckLevel()) == 0 {
		shardInfo.ClusterTimerAckLevel = map[string]*time.Time{
			clusterName: shardInfo.GetTimerAckLevelTime(),
		}
	}

	if shardInfo.GetClusterReplicationLevel() == nil {
		shardInfo.ClusterReplicationLevel = make(map[string]int64)
	}

	if shardInfo.GetReplicationDlqAckLevel() == nil {
		shardInfo.ReplicationDlqAckLevel = make(map[string]int64)
	}

	return shardInfo, nil
}

func NamespaceDetailToBlob(info *persistencespb.NamespaceDetail) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func NamespaceDetailFromBlob(b []byte, proto string) (*persistencespb.NamespaceDetail, error) {
	result := &persistencespb.NamespaceDetail{}
	return result, proto3Decode(b, proto, result)
}

func HistoryTreeInfoToBlob(info *persistencespb.HistoryTreeInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func HistoryTreeInfoFromBlob(b []byte, proto string) (*persistencespb.HistoryTreeInfo, error) {
	result := &persistencespb.HistoryTreeInfo{}
	return result, proto3Decode(b, proto, result)
}

func HistoryBranchToBlob(info *persistencespb.HistoryBranch) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func HistoryBranchFromBlob(b []byte, proto string) (*persistencespb.HistoryBranch, error) {
	result := &persistencespb.HistoryBranch{}
	return result, proto3Decode(b, proto, result)
}

func WorkflowExecutionInfoToBlob(info *persistencespb.WorkflowExecutionInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func WorkflowExecutionInfoFromBlob(b []byte, proto string) (*persistencespb.WorkflowExecutionInfo, error) {
	result := &persistencespb.WorkflowExecutionInfo{}
	return result, proto3Decode(b, proto, result)
}

func WorkflowExecutionStateToBlob(info *persistencespb.WorkflowExecutionState) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func WorkflowExecutionStateFromBlob(b []byte, proto string) (*persistencespb.WorkflowExecutionState, error) {
	result := &persistencespb.WorkflowExecutionState{}
	return result, proto3Decode(b, proto, result)
}

func ActivityInfoToBlob(info *persistencespb.ActivityInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ActivityInfoFromBlob(b []byte, proto string) (*persistencespb.ActivityInfo, error) {
	result := &persistencespb.ActivityInfo{}
	return result, proto3Decode(b, proto, result)
}

func ChildExecutionInfoToBlob(info *persistencespb.ChildExecutionInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ChildExecutionInfoFromBlob(b []byte, proto string) (*persistencespb.ChildExecutionInfo, error) {
	result := &persistencespb.ChildExecutionInfo{}
	return result, proto3Decode(b, proto, result)
}

func SignalInfoToBlob(info *persistencespb.SignalInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func SignalInfoFromBlob(b []byte, proto string) (*persistencespb.SignalInfo, error) {
	result := &persistencespb.SignalInfo{}
	return result, proto3Decode(b, proto, result)
}

func RequestCancelInfoToBlob(info *persistencespb.RequestCancelInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func RequestCancelInfoFromBlob(b []byte, proto string) (*persistencespb.RequestCancelInfo, error) {
	result := &persistencespb.RequestCancelInfo{}
	return result, proto3Decode(b, proto, result)
}

func TimerInfoToBlob(info *persistencespb.TimerInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TimerInfoFromBlob(b []byte, proto string) (*persistencespb.TimerInfo, error) {
	result := &persistencespb.TimerInfo{}
	return result, proto3Decode(b, proto, result)
}

func TaskInfoToBlob(info *persistencespb.AllocatedTaskInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TaskInfoFromBlob(b []byte, proto string) (*persistencespb.AllocatedTaskInfo, error) {
	result := &persistencespb.AllocatedTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func TaskQueueInfoToBlob(info *persistencespb.TaskQueueInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TaskQueueInfoFromBlob(b []byte, proto string) (*persistencespb.TaskQueueInfo, error) {
	result := &persistencespb.TaskQueueInfo{}
	return result, proto3Decode(b, proto, result)
}

func TransferTaskInfoToBlob(info *persistencespb.TransferTaskInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TransferTaskInfoFromBlob(b []byte, proto string) (*persistencespb.TransferTaskInfo, error) {
	result := &persistencespb.TransferTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func TimerTaskInfoToBlob(info *persistencespb.TimerTaskInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TimerTaskInfoFromBlob(b []byte, proto string) (*persistencespb.TimerTaskInfo, error) {
	result := &persistencespb.TimerTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func ReplicationTaskInfoToBlob(info *persistencespb.ReplicationTaskInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ReplicationTaskInfoFromBlob(b []byte, proto string) (*persistencespb.ReplicationTaskInfo, error) {
	result := &persistencespb.ReplicationTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func ReplicationVersionsToBlob(info *persistencespb.ReplicationVersions) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ReplicationVersionsFromBlob(b []byte, proto string) (*persistencespb.ReplicationVersions, error) {
	result := &persistencespb.ReplicationVersions{}
	return result, proto3Decode(b, proto, result)
}

func ChecksumToBlob(checksum *persistencespb.Checksum) (commonpb.DataBlob, error) {
	// nil is replaced with empty object because it is not supported for "checksum" field in DB.
	if checksum == nil {
		checksum = &persistencespb.Checksum{}
	}
	return proto3Encode(checksum)
}

func ChecksumFromBlob(b []byte, proto string) (*persistencespb.Checksum, error) {
	result := &persistencespb.Checksum{}
	err := proto3Decode(b, proto, result)
	if err != nil || result.GetFlavor() == enumsspb.CHECKSUM_FLAVOR_UNSPECIFIED {
		// If result is an empty struct (Flavor is unspecified), replace it with nil, because everywhere in the code checksum is pointer type.
		return nil, err
	}
	return result, nil
}

func proto3Encode(m proto.Marshaler) (commonpb.DataBlob, error) {
	blob := commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	data, err := m.Marshal()
	if err != nil {
		return blob, fmt.Errorf("error serializing struct to blob using %v encoding: %w", enumspb.ENCODING_TYPE_PROTO3, err)
	}
	blob.Data = data
	return blob, nil
}

func proto3Decode(b []byte, encoding string, result proto.Unmarshaler) error {
	if e, ok := enumspb.EncodingType_value[encoding]; !ok || enumspb.EncodingType(e) != enumspb.ENCODING_TYPE_PROTO3 {
		return fmt.Errorf("encoding %s doesn't match expected encoding %v", encoding, enumspb.ENCODING_TYPE_PROTO3)
	}

	if err := result.Unmarshal(b); err != nil {
		return fmt.Errorf("error deserializing blob using %v encoding: %w", enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return nil
}
