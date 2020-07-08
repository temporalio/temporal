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

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	enumspb "go.temporal.io/temporal-proto/enums/v1"

	commonpb "go.temporal.io/temporal-proto/common/v1"

	"github.com/temporalio/temporal/api/persistenceblobs/v1"
	"github.com/temporalio/temporal/common"
)

func validateProto(p string, expected common.EncodingType) error {
	if common.EncodingType(p) != expected {
		return fmt.Errorf("invalid encoding type: %v", p)
	}
	return nil
}

func encodeErr(encoding common.EncodingType, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("error serializing struct to blob using encoding - %v - : %v", encoding, err)
}

func decodeErr(encoding common.EncodingType, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("error deserializing blob to blob using encoding - %v - : %v", encoding, err)
}

func proto3Encode(m proto.Marshaler) (DataBlob, error) {
	blob := DataBlob{Encoding: common.EncodingTypeProto3}
	data, err := m.Marshal()
	if err != nil {
		return blob, encodeErr(common.EncodingTypeProto3, err)
	}
	blob.Data = data
	return blob, nil
}

func proto3Decode(b []byte, proto string, result proto.Unmarshaler) error {
	if err := validateProto(proto, common.EncodingTypeProto3); err != nil {
		return err
	}
	return decodeErr(common.EncodingTypeProto3, result.Unmarshal(b))
}

func ShardInfoToBlob(info *persistenceblobs.ShardInfo) (DataBlob, error) {
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
		shardInfo.ClusterTimerAckLevel = map[string]*types.Timestamp{
			clusterName: shardInfo.GetTimerAckLevel(),
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

func NamespaceDetailToBlob(info *persistenceblobs.NamespaceDetail) (DataBlob, error) {
	return proto3Encode(info)
}

func NamespaceDetailFromBlob(b []byte, proto string) (*persistenceblobs.NamespaceDetail, error) {
	result := &persistenceblobs.NamespaceDetail{}
	return result, proto3Decode(b, proto, result)
}

func HistoryTreeInfoToBlob(info *persistenceblobs.HistoryTreeInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func HistoryTreeInfoFromBlob(b []byte, proto string) (*persistenceblobs.HistoryTreeInfo, error) {
	result := &persistenceblobs.HistoryTreeInfo{}
	return result, proto3Decode(b, proto, result)
}

func HistoryBranchToBlob(info *persistenceblobs.HistoryBranch) (DataBlob, error) {
	return proto3Encode(info)
}

func HistoryBranchFromBlob(b []byte, proto string) (*persistenceblobs.HistoryBranch, error) {
	result := &persistenceblobs.HistoryBranch{}
	return result, proto3Decode(b, proto, result)
}

func WorkflowExecutionInfoToBlob(info *persistenceblobs.WorkflowExecutionInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func WorkflowExecutionInfoFromBlob(b []byte, proto string) (*persistenceblobs.WorkflowExecutionInfo, error) {
	result := &persistenceblobs.WorkflowExecutionInfo{}
	return result, proto3Decode(b, proto, result)
}

func WorkflowExecutionStateToBlob(info *persistenceblobs.WorkflowExecutionState) (DataBlob, error) {
	return proto3Encode(info)
}

func WorkflowExecutionStateFromBlob(b []byte, proto string) (*persistenceblobs.WorkflowExecutionState, error) {
	result := &persistenceblobs.WorkflowExecutionState{}
	return result, proto3Decode(b, proto, result)
}

func ActivityInfoToBlob(info *persistenceblobs.ActivityInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func ActivityInfoFromBlob(b []byte, proto string) (*persistenceblobs.ActivityInfo, error) {
	result := &persistenceblobs.ActivityInfo{}
	return result, proto3Decode(b, proto, result)
}

func ChildExecutionInfoToBlob(info *persistenceblobs.ChildExecutionInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func ChildExecutionInfoFromBlob(b []byte, proto string) (*persistenceblobs.ChildExecutionInfo, error) {
	result := &persistenceblobs.ChildExecutionInfo{}
	return result, proto3Decode(b, proto, result)
}

func SignalInfoToBlob(info *persistenceblobs.SignalInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func SignalInfoFromBlob(b []byte, proto string) (*persistenceblobs.SignalInfo, error) {
	result := &persistenceblobs.SignalInfo{}
	return result, proto3Decode(b, proto, result)
}

func RequestCancelInfoToBlob(info *persistenceblobs.RequestCancelInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func RequestCancelInfoFromBlob(b []byte, proto string) (*persistenceblobs.RequestCancelInfo, error) {
	result := &persistenceblobs.RequestCancelInfo{}
	return result, proto3Decode(b, proto, result)
}

func TimerInfoToBlob(info *persistenceblobs.TimerInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func TimerInfoFromBlob(b []byte, proto string) (*persistenceblobs.TimerInfo, error) {
	result := &persistenceblobs.TimerInfo{}
	return result, proto3Decode(b, proto, result)
}

func TaskInfoToBlob(info *persistenceblobs.AllocatedTaskInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func TaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.AllocatedTaskInfo, error) {
	result := &persistenceblobs.AllocatedTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func TaskQueueInfoToBlob(info *persistenceblobs.TaskQueueInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func TaskQueueInfoFromBlob(b []byte, proto string) (*persistenceblobs.TaskQueueInfo, error) {
	result := &persistenceblobs.TaskQueueInfo{}
	return result, proto3Decode(b, proto, result)
}

func TransferTaskInfoToBlob(info *persistenceblobs.TransferTaskInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func TransferTaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.TransferTaskInfo, error) {
	result := &persistenceblobs.TransferTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func TimerTaskInfoToBlob(info *persistenceblobs.TimerTaskInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func TimerTaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.TimerTaskInfo, error) {
	result := &persistenceblobs.TimerTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func ReplicationTaskInfoToBlob(info *persistenceblobs.ReplicationTaskInfo) (DataBlob, error) {
	return proto3Encode(info)
}

func ReplicationTaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.ReplicationTaskInfo, error) {
	result := &persistenceblobs.ReplicationTaskInfo{}
	return result, proto3Decode(b, proto, result)
}

func ReplicationVersionsToBlob(info *persistenceblobs.ReplicationVersions) (DataBlob, error) {
	return proto3Encode(info)
}

func ReplicationVersionsFromBlob(b []byte, proto string) (*persistenceblobs.ReplicationVersions, error) {
	result := &persistenceblobs.ReplicationVersions{}
	return result, proto3Decode(b, proto, result)
}

func ChecksumToBlob(info *persistenceblobs.Checksum) (DataBlob, error) {
	return proto3Encode(info)
}

func ChecksumFromBlob(b []byte, proto string) (*persistenceblobs.Checksum, error) {
	result := &persistenceblobs.Checksum{}
	return result, proto3Decode(b, proto, result)
}

type DataBlob struct {
	Encoding common.EncodingType
	Data     []byte
}

// ToProto convert data blob to proto representation
func (d *DataBlob) ToProto() *commonpb.DataBlob {
	switch d.Encoding {
	case common.EncodingTypeJSON:
		return &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_JSON,
			Data:         d.Data,
		}
	case common.EncodingTypeProto3:
		return &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         d.Data,
		}
	default:
		panic(fmt.Sprintf("DataBlob seeing unsupported enconding type: %v", d.Encoding))
	}
}

// GetEncoding returns encoding type
func (d *DataBlob) GetEncoding() common.EncodingType {
	encodingStr := string(d.Encoding)

	switch common.EncodingType(encodingStr) {
	case common.EncodingTypeProto3:
		return common.EncodingTypeProto3
	case common.EncodingTypeGob:
		return common.EncodingTypeGob
	case common.EncodingTypeJSON:
		return common.EncodingTypeJSON
	case common.EncodingTypeEmpty:
		return common.EncodingTypeEmpty
	default:
		return common.EncodingTypeUnknown
	}
}
