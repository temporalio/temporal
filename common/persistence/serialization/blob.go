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

	"go.temporal.io/server/api/persistenceblobs/v1"
)

func ShardInfoToBlob(info *persistenceblobs.ShardInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ShardInfoFromBlob(b []byte, proto string, clusterName string) (*persistenceblobs.ShardInfo, error) {
	shardInfo := &persistenceblobs.ShardInfo{}
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

func NamespaceDetailToBlob(info *persistenceblobs.NamespaceDetail) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func NamespaceDetailFromBlob(b []byte, proto string) (*persistenceblobs.NamespaceDetail, error) {
	result := &persistenceblobs.NamespaceDetail{}
	return result, proto3Decode(b, proto, result)
}

func HistoryTreeInfoToBlob(info *persistenceblobs.HistoryTreeInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func HistoryTreeInfoFromBlob(b []byte, proto string) (*persistenceblobs.HistoryTreeInfo, error) {
	result := &persistenceblobs.HistoryTreeInfo{}
	return result, proto3Decode(b, proto, result)
}

func HistoryBranchToBlob(info *persistenceblobs.HistoryBranch) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func HistoryBranchFromBlob(b []byte, proto string) (*persistenceblobs.HistoryBranch, error) {
	result := &persistenceblobs.HistoryBranch{}
	return result, proto3Decode(b, proto, result)
}

func WorkflowExecutionInfoToBlob(info *persistenceblobs.WorkflowExecutionInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func WorkflowExecutionInfoFromBlob(b []byte, proto string) (*persistenceblobs.WorkflowExecutionInfo, error) {
	result := &persistenceblobs.WorkflowExecutionInfo{}
	return result, proto3Decode(b, proto, result)
}

func WorkflowExecutionStateToBlob(info *persistenceblobs.WorkflowExecutionState) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func WorkflowExecutionStateFromBlob(b []byte, proto string) (*persistenceblobs.WorkflowExecutionState, error) {
	result := &persistenceblobs.WorkflowExecutionState{}
	return result, proto3Decode(b, proto, result)
}

func ActivityInfoToBlob(info *persistenceblobs.ActivityInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ActivityInfoFromBlob(b []byte, proto string) (*persistenceblobs.ActivityInfo, error) {
	result := &persistenceblobs.ActivityInfo{}
	return result, proto3Decode(b, proto, result)
}

func ChildExecutionInfoToBlob(info *persistenceblobs.ChildExecutionInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ChildExecutionInfoFromBlob(b []byte, proto string) (*persistenceblobs.ChildExecutionInfo, error) {
	result := &persistenceblobs.ChildExecutionInfo{}
	return result, proto3Decode(b, proto, result)
}

func SignalInfoToBlob(info *persistenceblobs.SignalInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func SignalInfoFromBlob(b []byte, proto string) (*persistenceblobs.SignalInfo, error) {
	result := &persistenceblobs.SignalInfo{}
	return result, proto3Decode(b, proto, result)
}

func RequestCancelInfoToBlob(info *persistenceblobs.RequestCancelInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func RequestCancelInfoFromBlob(b []byte, proto string) (*persistenceblobs.RequestCancelInfo, error) {
	result := &persistenceblobs.RequestCancelInfo{}
	return result, proto3Decode(b, proto, result)
}

func TimerInfoToBlob(info *persistenceblobs.TimerInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TimerInfoFromBlob(b []byte, proto string) (*persistenceblobs.TimerInfo, error) {
	result := &persistenceblobs.TimerInfo{}
	return result, proto3Decode(b, proto, result)
}

func TaskInfoToBlob(info *persistenceblobs.AllocatedTaskInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.AllocatedTaskInfo, error) {
	result := &persistenceblobs.AllocatedTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func TaskQueueInfoToBlob(info *persistenceblobs.TaskQueueInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TaskQueueInfoFromBlob(b []byte, proto string) (*persistenceblobs.TaskQueueInfo, error) {
	result := &persistenceblobs.TaskQueueInfo{}
	return result, proto3Decode(b, proto, result)
}

func TransferTaskInfoToBlob(info *persistenceblobs.TransferTaskInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TransferTaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.TransferTaskInfo, error) {
	result := &persistenceblobs.TransferTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func TimerTaskInfoToBlob(info *persistenceblobs.TimerTaskInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func TimerTaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.TimerTaskInfo, error) {
	result := &persistenceblobs.TimerTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func ReplicationTaskInfoToBlob(info *persistenceblobs.ReplicationTaskInfo) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ReplicationTaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.ReplicationTaskInfo, error) {
	result := &persistenceblobs.ReplicationTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func ReplicationVersionsToBlob(info *persistenceblobs.ReplicationVersions) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ReplicationVersionsFromBlob(b []byte, proto string) (*persistenceblobs.ReplicationVersions, error) {
	result := &persistenceblobs.ReplicationVersions{}
	return result, proto3Decode(b, proto, result)
}

func ChecksumToBlob(info *persistenceblobs.Checksum) (commonpb.DataBlob, error) {
	return proto3Encode(info)
}

func ChecksumFromBlob(b []byte, proto string) (*persistenceblobs.Checksum, error) {
	result := &persistenceblobs.Checksum{}
	return result, proto3Decode(b, proto, result)
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
