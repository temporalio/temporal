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

package serialization

import (
	"fmt"

	"github.com/gogo/protobuf/types"

	"go.uber.org/thriftrw/wire"

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
)

// thriftRWType represents an thrift auto generated type
type thriftRWType interface {
	ToWire() (wire.Value, error)
	FromWire(w wire.Value) error
}

type ProtoMarshal interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

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

func protoRWEncode(m ProtoMarshal) (DataBlob, error) {
	blob := DataBlob{Encoding: common.EncodingTypeProto3}
	data, err := m.Marshal()
	if err != nil {
		return blob, encodeErr(common.EncodingTypeProto3, err)
	}
	blob.Data = data
	return blob, nil
}

func protoRWDecode(b []byte, proto string, result ProtoMarshal) error {
	if err := validateProto(proto, common.EncodingTypeProto3); err != nil {
		return err
	}
	return decodeErr(common.EncodingTypeProto3, result.Unmarshal(b))
}

func ShardInfoToBlob(info *persistenceblobs.ShardInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func ShardInfoFromBlob(b []byte, proto string, clusterName string) (*persistenceblobs.ShardInfo, error) {
	shardInfo := &persistenceblobs.ShardInfo{}
	err := protoRWDecode(b, proto, shardInfo)

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

	if shardInfo.GetReplicationDLQAckLevel() == nil {
		shardInfo.ReplicationDLQAckLevel = make(map[string]int64)
	}

	return shardInfo, nil
}

func DomainInfoToBlob(info *persistenceblobs.DomainInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func DomainInfoFromBlob(b []byte, proto string) (*persistenceblobs.DomainInfo, error) {
	result := &persistenceblobs.DomainInfo{}
	return result, protoRWDecode(b, proto, result)
}

func HistoryTreeInfoToBlob(info *persistenceblobs.HistoryTreeInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func HistoryTreeInfoFromBlob(b []byte, proto string) (*persistenceblobs.HistoryTreeInfo, error) {
	result := &persistenceblobs.HistoryTreeInfo{}
	return result, protoRWDecode(b, proto, result)
}

func HistoryBranchToBlob(info *persistenceblobs.HistoryBranch) (DataBlob, error) {
	return protoRWEncode(info)
}

func HistoryBranchFromBlob(b []byte, proto string) (*persistenceblobs.HistoryBranch, error) {
	result := &persistenceblobs.HistoryBranch{}
	return result, protoRWDecode(b, proto, result)
}

func WorkflowExecutionInfoToBlob(info *persistenceblobs.WorkflowExecutionInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func WorkflowExecutionInfoFromBlob(b []byte, proto string) (*persistenceblobs.WorkflowExecutionInfo, error) {
	result := &persistenceblobs.WorkflowExecutionInfo{}
	return result, protoRWDecode(b, proto, result)
}

func WorkflowExecutionStateToBlob(info *persistenceblobs.WorkflowExecutionState) (DataBlob, error) {
	return protoRWEncode(info)
}

func WorkflowExecutionStateFromBlob(b []byte, proto string) (*persistenceblobs.WorkflowExecutionState, error) {
	result := &persistenceblobs.WorkflowExecutionState{}
	return result, protoRWDecode(b, proto, result)
}

func ActivityInfoToBlob(info *persistenceblobs.ActivityInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func ActivityInfoFromBlob(b []byte, proto string) (*persistenceblobs.ActivityInfo, error) {
	result := &persistenceblobs.ActivityInfo{}
	return result, protoRWDecode(b, proto, result)
}

func ChildExecutionInfoToBlob(info *persistenceblobs.ChildExecutionInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func ChildExecutionInfoFromBlob(b []byte, proto string) (*persistenceblobs.ChildExecutionInfo, error) {
	result := &persistenceblobs.ChildExecutionInfo{}
	return result, protoRWDecode(b, proto, result)
}

func SignalInfoToBlob(info *persistenceblobs.SignalInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func SignalInfoFromBlob(b []byte, proto string) (*persistenceblobs.SignalInfo, error) {
	result := &persistenceblobs.SignalInfo{}
	return result, protoRWDecode(b, proto, result)
}

func RequestCancelInfoToBlob(info *persistenceblobs.RequestCancelInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func RequestCancelInfoFromBlob(b []byte, proto string) (*persistenceblobs.RequestCancelInfo, error) {
	result := &persistenceblobs.RequestCancelInfo{}
	return result, protoRWDecode(b, proto, result)
}

func TimerInfoToBlob(info *persistenceblobs.TimerInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func TimerInfoFromBlob(b []byte, proto string) (*persistenceblobs.TimerInfo, error) {
	result := &persistenceblobs.TimerInfo{}
	return result, protoRWDecode(b, proto, result)
}

func TaskInfoToBlob(info *persistenceblobs.AllocatedTaskInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func TaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.AllocatedTaskInfo, error) {
	result := &persistenceblobs.AllocatedTaskInfo{}
	return result, protoRWDecode(b, proto, result)
}

func TaskListInfoToBlob(info *persistenceblobs.TaskListInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func TaskListInfoFromBlob(b []byte, proto string) (*persistenceblobs.TaskListInfo, error) {
	result := &persistenceblobs.TaskListInfo{}
	return result, protoRWDecode(b, proto, result)
}

func TransferTaskInfoToBlob(info *persistenceblobs.TransferTaskInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func TransferTaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.TransferTaskInfo, error) {
	result := &persistenceblobs.TransferTaskInfo{}
	return result, protoRWDecode(b, proto, result)
}

func TimerTaskInfoToBlob(info *persistenceblobs.TimerTaskInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func TimerTaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.TimerTaskInfo, error) {
	result := &persistenceblobs.TimerTaskInfo{}
	return result, protoRWDecode(b, proto, result)
}

func ReplicationTaskInfoToBlob(info *persistenceblobs.ReplicationTaskInfo) (DataBlob, error) {
	return protoRWEncode(info)
}

func ReplicationTaskInfoFromBlob(b []byte, proto string) (*persistenceblobs.ReplicationTaskInfo, error) {
	result := &persistenceblobs.ReplicationTaskInfo{}
	return result, protoRWDecode(b, proto, result)
}

func ChecksumToBlob(info *persistenceblobs.Checksum) (DataBlob, error) {
	return protoRWEncode(info)
}

func ChecksumFromBlob(b []byte, proto string) (*persistenceblobs.Checksum, error) {
	result := &persistenceblobs.Checksum{}
	return result, protoRWDecode(b, proto, result)
}

type DataBlob struct {
	Encoding common.EncodingType
	Data     []byte
}

// ToProto convert data blob to thrift representation
func (d *DataBlob) ToProto() *commonproto.DataBlob {
	switch d.Encoding {
	case common.EncodingTypeJSON:
		return &commonproto.DataBlob{
			EncodingType: enums.EncodingTypeJSON,
			Data:         d.Data,
		}
	case common.EncodingTypeThriftRW:
		return &commonproto.DataBlob{
			EncodingType: enums.EncodingTypeThriftRW,
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
	case common.EncodingTypeGob:
		return common.EncodingTypeGob
	case common.EncodingTypeJSON:
		return common.EncodingTypeJSON
	case common.EncodingTypeThriftRW:
		return common.EncodingTypeThriftRW
	case common.EncodingTypeEmpty:
		return common.EncodingTypeEmpty
	default:
		return common.EncodingTypeUnknown
	}
}
