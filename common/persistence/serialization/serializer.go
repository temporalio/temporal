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
	"reflect"
	"time"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// Serializer is used by persistence to serialize/deserialize objects
	// It will only be used inside persistence, so that serialize/deserialize is transparent for application
	Serializer interface {
		SerializeEvents(batch []*historypb.HistoryEvent, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		DeserializeEvents(data *commonpb.DataBlob) ([]*historypb.HistoryEvent, error)

		SerializeEvent(event *historypb.HistoryEvent, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		DeserializeEvent(data *commonpb.DataBlob) (*historypb.HistoryEvent, error)

		SerializeClusterMetadata(icm *persistencespb.ClusterMetadata, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		DeserializeClusterMetadata(data *commonpb.DataBlob) (*persistencespb.ClusterMetadata, error)

		ShardInfoToBlob(info *persistencespb.ShardInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		ShardInfoFromBlob(data *commonpb.DataBlob, clusterName string) (*persistencespb.ShardInfo, error)

		NamespaceDetailToBlob(info *persistencespb.NamespaceDetail, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		NamespaceDetailFromBlob(data *commonpb.DataBlob) (*persistencespb.NamespaceDetail, error)

		HistoryTreeInfoToBlob(info *persistencespb.HistoryTreeInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		HistoryTreeInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryTreeInfo, error)

		HistoryBranchToBlob(info *persistencespb.HistoryBranch, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		HistoryBranchFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryBranch, error)

		WorkflowExecutionInfoToBlob(info *persistencespb.WorkflowExecutionInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		WorkflowExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionInfo, error)

		WorkflowExecutionStateToBlob(info *persistencespb.WorkflowExecutionState, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		WorkflowExecutionStateFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error)

		ActivityInfoToBlob(info *persistencespb.ActivityInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		ActivityInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ActivityInfo, error)

		ChildExecutionInfoToBlob(info *persistencespb.ChildExecutionInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		ChildExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ChildExecutionInfo, error)

		SignalInfoToBlob(info *persistencespb.SignalInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		SignalInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.SignalInfo, error)

		RequestCancelInfoToBlob(info *persistencespb.RequestCancelInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		RequestCancelInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.RequestCancelInfo, error)

		TimerInfoToBlob(info *persistencespb.TimerInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		TimerInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerInfo, error)

		TaskInfoToBlob(info *persistencespb.AllocatedTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		TaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.AllocatedTaskInfo, error)

		TaskQueueInfoToBlob(info *persistencespb.TaskQueueInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		TaskQueueInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueInfo, error)

		ChecksumToBlob(checksum *persistencespb.Checksum, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		ChecksumFromBlob(data *commonpb.DataBlob) (*persistencespb.Checksum, error)

		QueueMetadataToBlob(metadata *persistencespb.QueueMetadata, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		QueueMetadataFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueMetadata, error)

		ReplicationTaskToBlob(replicationTask *replicationspb.ReplicationTask, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		ReplicationTaskFromBlob(data *commonpb.DataBlob) (*replicationspb.ReplicationTask, error)

		SerializeTransferTasks(taskSlice []tasks.Task) (map[tasks.Key]commonpb.DataBlob, error)
		DeserializeTransferTasks(blobSlice []commonpb.DataBlob) ([]tasks.Task, error)

		SerializeTimerTasks(taskSlice []tasks.Task) (map[tasks.Key]commonpb.DataBlob, error)
		DeserializeTimerTasks(blobSlice []commonpb.DataBlob) ([]tasks.Task, error)
		SerializeVisibilityTasks(taskSlice []tasks.Task) (map[tasks.Key]commonpb.DataBlob, error)
		DeserializeVisibilityTasks(blobSlice []commonpb.DataBlob) ([]tasks.Task, error)
		SerializeReplicationTasks(taskSlice []tasks.Task) (map[tasks.Key]commonpb.DataBlob, error)
		DeserializeReplicationTasks(blobSlice []commonpb.DataBlob) ([]tasks.Task, error)

		// TODO deprecate method below in favor of methods above (tasks)

		TransferTaskInfoToBlob(info *persistencespb.TransferTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		TransferTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TransferTaskInfo, error)
		TimerTaskInfoToBlob(info *persistencespb.TimerTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		TimerTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerTaskInfo, error)
		ReplicationTaskInfoToBlob(info *persistencespb.ReplicationTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		ReplicationTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ReplicationTaskInfo, error)
		VisibilityTaskInfoToBlob(info *persistencespb.VisibilityTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error)
		VisibilityTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.VisibilityTaskInfo, error)
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

	serializerImpl struct {
		TaskSerializer
	}
)

// NewSerializer returns a PayloadSerializer
func NewSerializer() Serializer {
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
func NewSerializationError(msg string) error {
	return &SerializationError{msg: msg}
}

func (e *SerializationError) Error() string {
	return fmt.Sprintf("serialization error: %v", e.msg)
}

// NewDeserializationError returns a DeserializationError
func NewDeserializationError(msg string) error {
	return &DeserializationError{msg: msg}
}

func (e *DeserializationError) Error() string {
	return fmt.Sprintf("deserialization error: %v", e.msg)
}

func (t *serializerImpl) ShardInfoToBlob(info *persistencespb.ShardInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) ShardInfoFromBlob(data *commonpb.DataBlob, clusterName string) (*persistencespb.ShardInfo, error) {
	shardInfo := &persistencespb.ShardInfo{}
	err := proto3DecodeBlob(data, shardInfo)

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

func (t *serializerImpl) NamespaceDetailToBlob(info *persistencespb.NamespaceDetail, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) NamespaceDetailFromBlob(data *commonpb.DataBlob) (*persistencespb.NamespaceDetail, error) {
	result := &persistencespb.NamespaceDetail{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) HistoryTreeInfoToBlob(info *persistencespb.HistoryTreeInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) HistoryTreeInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryTreeInfo, error) {
	result := &persistencespb.HistoryTreeInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) HistoryBranchToBlob(info *persistencespb.HistoryBranch, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) HistoryBranchFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryBranch, error) {
	result := &persistencespb.HistoryBranch{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) WorkflowExecutionInfoToBlob(info *persistencespb.WorkflowExecutionInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) WorkflowExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionInfo, error) {
	result := &persistencespb.WorkflowExecutionInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) WorkflowExecutionStateToBlob(info *persistencespb.WorkflowExecutionState, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) WorkflowExecutionStateFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error) {
	result := &persistencespb.WorkflowExecutionState{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) ActivityInfoToBlob(info *persistencespb.ActivityInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) ActivityInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ActivityInfo, error) {
	result := &persistencespb.ActivityInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) ChildExecutionInfoToBlob(info *persistencespb.ChildExecutionInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) ChildExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ChildExecutionInfo, error) {
	result := &persistencespb.ChildExecutionInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) SignalInfoToBlob(info *persistencespb.SignalInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) SignalInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.SignalInfo, error) {
	result := &persistencespb.SignalInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) RequestCancelInfoToBlob(info *persistencespb.RequestCancelInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) RequestCancelInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.RequestCancelInfo, error) {
	result := &persistencespb.RequestCancelInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) TimerInfoToBlob(info *persistencespb.TimerInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) TimerInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerInfo, error) {
	result := &persistencespb.TimerInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) TaskInfoToBlob(info *persistencespb.AllocatedTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) TaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.AllocatedTaskInfo, error) {
	result := &persistencespb.AllocatedTaskInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) TaskQueueInfoToBlob(info *persistencespb.TaskQueueInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) TaskQueueInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueInfo, error) {
	result := &persistencespb.TaskQueueInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) TransferTaskInfoToBlob(info *persistencespb.TransferTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) TransferTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TransferTaskInfo, error) {
	result := &persistencespb.TransferTaskInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) TimerTaskInfoToBlob(info *persistencespb.TimerTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) TimerTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerTaskInfo, error) {
	result := &persistencespb.TimerTaskInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) ReplicationTaskInfoToBlob(info *persistencespb.ReplicationTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) ReplicationTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ReplicationTaskInfo, error) {
	result := &persistencespb.ReplicationTaskInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) VisibilityTaskInfoToBlob(info *persistencespb.VisibilityTaskInfo, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(info, encodingType)
}

func (t *serializerImpl) VisibilityTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.VisibilityTaskInfo, error) {
	result := &persistencespb.VisibilityTaskInfo{}
	return result, proto3DecodeBlob(data, result)
}

func (t *serializerImpl) ChecksumToBlob(checksum *persistencespb.Checksum, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	// nil is replaced with empty object because it is not supported for "checksum" field in DB.
	if checksum == nil {
		checksum = &persistencespb.Checksum{}
	}
	return proto3EncodeBlob(checksum, encodingType)
}

func (t *serializerImpl) ChecksumFromBlob(data *commonpb.DataBlob) (*persistencespb.Checksum, error) {
	result := &persistencespb.Checksum{}
	err := proto3DecodeBlob(data, result)
	if err != nil || result.GetFlavor() == enumsspb.CHECKSUM_FLAVOR_UNSPECIFIED {
		// If result is an empty struct (Flavor is unspecified), replace it with nil, because everywhere in the code checksum is pointer type.
		return nil, err
	}
	return result, nil
}

func (t *serializerImpl) QueueMetadataToBlob(metadata *persistencespb.QueueMetadata, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return encodeBlob(metadata, encodingType)
}

func (t *serializerImpl) QueueMetadataFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueMetadata, error) {
	result := &persistencespb.QueueMetadata{}
	return result, decodeBlob(data, result)
}

func (t *serializerImpl) ReplicationTaskToBlob(replicationTask *replicationspb.ReplicationTask, encodingType enumspb.EncodingType) (*commonpb.DataBlob, error) {
	return proto3EncodeBlob(replicationTask, encodingType)
}

func (t *serializerImpl) ReplicationTaskFromBlob(data *commonpb.DataBlob) (*replicationspb.ReplicationTask, error) {
	result := &replicationspb.ReplicationTask{}
	return result, proto3DecodeBlob(data, result)
}

func proto3DecodeBlob(data *commonpb.DataBlob, result proto.Message) error {
	if data == nil {
		// TODO: should we return nil or error?
		return NewDeserializationError("cannot decode nil")
	}

	if data.EncodingType != enumspb.ENCODING_TYPE_PROTO3 {
		return NewDeserializationError(fmt.Sprintf("encoding %v doesn't match expected encoding %v", data.EncodingType, enumspb.ENCODING_TYPE_PROTO3))
	}

	if err := proto.Unmarshal(data.Data, result); err != nil {
		return NewDeserializationError(fmt.Sprintf("error deserializing blob using %v encoding: %s", enumspb.ENCODING_TYPE_PROTO3, err))
	}
	return nil
}

func decodeBlob(data *commonpb.DataBlob, result proto.Message) error {
	if data == nil {
		// TODO: should we return nil or error?
		return NewDeserializationError("cannot decode nil")
	}

	if data.Data == nil {
		return nil
	}

	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_JSON:
		return codec.NewJSONPBEncoder().Decode(data.Data, result)
	case enumspb.ENCODING_TYPE_PROTO3:
		return proto3DecodeBlob(data, result)
	default:
		return NewUnknownEncodingTypeError(data.EncodingType)
	}
}

func encodeBlob(o proto.Message, encoding enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if o == nil || (reflect.ValueOf(o).Kind() == reflect.Ptr && reflect.ValueOf(o).IsNil()) {
		return &commonpb.DataBlob{
			Data:         nil,
			EncodingType: encoding,
		}, nil
	}

	switch encoding {
	case enumspb.ENCODING_TYPE_JSON:
		blob, err := codec.NewJSONPBEncoder().Encode(o.(proto.Message))
		if err != nil {
			return nil, err
		}
		return &commonpb.DataBlob{
			Data:         blob,
			EncodingType: enumspb.ENCODING_TYPE_JSON,
		}, nil
	case enumspb.ENCODING_TYPE_PROTO3:
		return proto3EncodeBlob(o, enumspb.ENCODING_TYPE_PROTO3)
	default:
		return nil, NewUnknownEncodingTypeError(encoding)
	}
}

func proto3EncodeBlob(m proto.Message, encoding enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if encoding != enumspb.ENCODING_TYPE_PROTO3 {
		return nil, NewUnknownEncodingTypeError(encoding)
	}

	if m == nil || (reflect.ValueOf(m).Kind() == reflect.Ptr && reflect.ValueOf(m).IsNil()) {
		// TODO: is this expected?
		return &commonpb.DataBlob{
			Data:         nil,
			EncodingType: encoding,
		}, nil
	}

	blob := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, NewSerializationError(err.Error())
	}
	blob.Data = data
	return blob, nil
}
