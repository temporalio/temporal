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

package sql

import (
	"bytes"
	"fmt"

	"github.com/gogo/protobuf/types"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"

	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/wire"

	"github.com/temporalio/temporal/.gen/go/sqlblobs"
	"github.com/temporalio/temporal/common"
	p "github.com/temporalio/temporal/common/persistence"
)

// thriftRWType represents an thrift auto generated type
type thriftRWType interface {
	ToWire() (wire.Value, error)
	FromWire(w wire.Value) error
}

type protoMarshal interface {
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

func protoRWEncode(m protoMarshal) (p.DataBlob, error) {
	blob := p.DataBlob{Encoding: common.EncodingTypeProto3}
	data, err := m.Marshal()
	if err != nil {
		return blob, encodeErr(common.EncodingTypeProto3, err)
	}
	blob.Data = data
	return blob, nil
}

func protoRWDecode(b []byte, proto string, result protoMarshal) error {
	if err := validateProto(proto, common.EncodingTypeProto3); err != nil {
		return err
	}
	return decodeErr(common.EncodingTypeThriftRW, result.Unmarshal(b))
}

func thriftRWEncode(t thriftRWType) (p.DataBlob, error) {
	blob := p.DataBlob{Encoding: common.EncodingTypeThriftRW}
	value, err := t.ToWire()
	if err != nil {
		return blob, encodeErr(common.EncodingTypeThriftRW, err)
	}
	var b bytes.Buffer
	if err := protocol.Binary.Encode(value, &b); err != nil {
		return blob, encodeErr(common.EncodingTypeThriftRW, err)
	}
	blob.Data = b.Bytes()
	return blob, nil
}

func thriftRWDecode(b []byte, proto string, result thriftRWType) error {
	if err := validateProto(proto, common.EncodingTypeThriftRW); err != nil {
		return err
	}
	value, err := protocol.Binary.Decode(bytes.NewReader(b), wire.TStruct)
	if err != nil {
		return decodeErr(common.EncodingTypeThriftRW, err)
	}
	return decodeErr(common.EncodingTypeThriftRW, result.FromWire(value))
}

func ShardInfoToBlob(info *persistenceblobs.ShardInfo) (p.DataBlob, error) {
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

	return shardInfo, nil
}

func domainInfoToBlob(info *sqlblobs.DomainInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func domainInfoFromBlob(b []byte, proto string) (*sqlblobs.DomainInfo, error) {
	result := &sqlblobs.DomainInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func historyTreeInfoToBlob(info *sqlblobs.HistoryTreeInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func historyTreeInfoFromBlob(b []byte, proto string) (*sqlblobs.HistoryTreeInfo, error) {
	result := &sqlblobs.HistoryTreeInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func workflowExecutionInfoToBlob(info *sqlblobs.WorkflowExecutionInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func workflowExecutionInfoFromBlob(b []byte, proto string) (*sqlblobs.WorkflowExecutionInfo, error) {
	result := &sqlblobs.WorkflowExecutionInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func activityInfoToBlob(info *sqlblobs.ActivityInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func activityInfoFromBlob(b []byte, proto string) (*sqlblobs.ActivityInfo, error) {
	result := &sqlblobs.ActivityInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func childExecutionInfoToBlob(info *sqlblobs.ChildExecutionInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func childExecutionInfoFromBlob(b []byte, proto string) (*sqlblobs.ChildExecutionInfo, error) {
	result := &sqlblobs.ChildExecutionInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func signalInfoToBlob(info *sqlblobs.SignalInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func signalInfoFromBlob(b []byte, proto string) (*sqlblobs.SignalInfo, error) {
	result := &sqlblobs.SignalInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func requestCancelInfoToBlob(info *sqlblobs.RequestCancelInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func requestCancelInfoFromBlob(b []byte, proto string) (*sqlblobs.RequestCancelInfo, error) {
	result := &sqlblobs.RequestCancelInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func timerInfoToBlob(info *sqlblobs.TimerInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func timerInfoFromBlob(b []byte, proto string) (*sqlblobs.TimerInfo, error) {
	result := &sqlblobs.TimerInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func taskInfoToBlob(info *sqlblobs.TaskInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func taskInfoFromBlob(b []byte, proto string) (*sqlblobs.TaskInfo, error) {
	result := &sqlblobs.TaskInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func taskListInfoToBlob(info *sqlblobs.TaskListInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func taskListInfoFromBlob(b []byte, proto string) (*sqlblobs.TaskListInfo, error) {
	result := &sqlblobs.TaskListInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func transferTaskInfoToBlob(info *sqlblobs.TransferTaskInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func transferTaskInfoFromBlob(b []byte, proto string) (*sqlblobs.TransferTaskInfo, error) {
	result := &sqlblobs.TransferTaskInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func timerTaskInfoToBlob(info *sqlblobs.TimerTaskInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func timerTaskInfoFromBlob(b []byte, proto string) (*sqlblobs.TimerTaskInfo, error) {
	result := &sqlblobs.TimerTaskInfo{}
	return result, thriftRWDecode(b, proto, result)
}

func replicationTaskInfoToBlob(info *sqlblobs.ReplicationTaskInfo) (p.DataBlob, error) {
	return thriftRWEncode(info)
}

func replicationTaskInfoFromBlob(b []byte, proto string) (*sqlblobs.ReplicationTaskInfo, error) {
	result := &sqlblobs.ReplicationTaskInfo{}
	return result, thriftRWDecode(b, proto, result)
}
