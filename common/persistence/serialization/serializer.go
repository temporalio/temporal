package serialization

import (
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	// Serializer is used by persistence to serialize/deserialize objects
	// It will only be used inside persistence, so that serialize/deserialize is transparent for application
	Serializer interface {
		SerializeEvents(batch []*historypb.HistoryEvent) (*commonpb.DataBlob, error)
		DeserializeEvents(data *commonpb.DataBlob) ([]*historypb.HistoryEvent, error)

		SerializeEvent(event *historypb.HistoryEvent) (*commonpb.DataBlob, error)
		DeserializeEvent(data *commonpb.DataBlob) (*historypb.HistoryEvent, error)
		DeserializeStrippedEvents(data *commonpb.DataBlob) ([]*historyspb.StrippedHistoryEvent, error)

		SerializeClusterMetadata(icm *persistencespb.ClusterMetadata) (*commonpb.DataBlob, error)
		DeserializeClusterMetadata(data *commonpb.DataBlob) (*persistencespb.ClusterMetadata, error)

		ShardInfoToBlob(info *persistencespb.ShardInfo) (*commonpb.DataBlob, error)
		ShardInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ShardInfo, error)

		NamespaceDetailToBlob(info *persistencespb.NamespaceDetail) (*commonpb.DataBlob, error)
		NamespaceDetailFromBlob(data *commonpb.DataBlob) (*persistencespb.NamespaceDetail, error)

		HistoryTreeInfoToBlob(info *persistencespb.HistoryTreeInfo) (*commonpb.DataBlob, error)
		HistoryTreeInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryTreeInfo, error)

		HistoryBranchToBlob(info *persistencespb.HistoryBranch) (*commonpb.DataBlob, error)
		HistoryBranchFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryBranch, error)

		WorkflowExecutionInfoToBlob(info *persistencespb.WorkflowExecutionInfo) (*commonpb.DataBlob, error)
		WorkflowExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionInfo, error)

		WorkflowExecutionStateToBlob(info *persistencespb.WorkflowExecutionState) (*commonpb.DataBlob, error)
		WorkflowExecutionStateFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error)

		ActivityInfoToBlob(info *persistencespb.ActivityInfo) (*commonpb.DataBlob, error)
		ActivityInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ActivityInfo, error)

		ChildExecutionInfoToBlob(info *persistencespb.ChildExecutionInfo) (*commonpb.DataBlob, error)
		ChildExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ChildExecutionInfo, error)

		SignalInfoToBlob(info *persistencespb.SignalInfo) (*commonpb.DataBlob, error)
		SignalInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.SignalInfo, error)

		RequestCancelInfoToBlob(info *persistencespb.RequestCancelInfo) (*commonpb.DataBlob, error)
		RequestCancelInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.RequestCancelInfo, error)

		TimerInfoToBlob(info *persistencespb.TimerInfo) (*commonpb.DataBlob, error)
		TimerInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerInfo, error)

		TaskInfoToBlob(info *persistencespb.AllocatedTaskInfo) (*commonpb.DataBlob, error)
		TaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.AllocatedTaskInfo, error)

		TaskQueueInfoToBlob(info *persistencespb.TaskQueueInfo) (*commonpb.DataBlob, error)
		TaskQueueInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueInfo, error)

		TaskQueueUserDataToBlob(info *persistencespb.TaskQueueUserData) (*commonpb.DataBlob, error)
		TaskQueueUserDataFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueUserData, error)

		ChecksumToBlob(checksum *persistencespb.Checksum) (*commonpb.DataBlob, error)
		ChecksumFromBlob(data *commonpb.DataBlob) (*persistencespb.Checksum, error)

		QueueMetadataToBlob(metadata *persistencespb.QueueMetadata) (*commonpb.DataBlob, error)
		QueueMetadataFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueMetadata, error)

		ReplicationTaskToBlob(replicationTask *replicationspb.ReplicationTask) (*commonpb.DataBlob, error)
		ReplicationTaskFromBlob(data *commonpb.DataBlob) (*replicationspb.ReplicationTask, error)
		// ParseReplicationTask is unique among these methods in that it does not serialize or deserialize a type to or
		// from a byte array. Instead, it takes a proto and "parses" it into a more structured type.
		ParseReplicationTask(replicationTask *persistencespb.ReplicationTaskInfo) (tasks.Task, error)
		// ParseReplicationTaskInfo is unique among these methods in that it does not serialize or deserialize a type to or
		// from a byte array. Instead, it takes a structured type and "parses" it into proto
		ParseReplicationTaskInfo(task tasks.Task) (*persistencespb.ReplicationTaskInfo, error)

		SerializeTask(task tasks.Task) (*commonpb.DataBlob, error)
		DeserializeTask(category tasks.Category, blob *commonpb.DataBlob) (tasks.Task, error)

		NexusEndpointToBlob(endpoint *persistencespb.NexusEndpoint) (*commonpb.DataBlob, error)
		NexusEndpointFromBlob(data *commonpb.DataBlob) (*persistencespb.NexusEndpoint, error)

		// ChasmNodeToBlob returns a single encoded blob for the node.
		ChasmNodeToBlob(node *persistencespb.ChasmNode) (*commonpb.DataBlob, error)
		ChasmNodeFromBlob(blob *commonpb.DataBlob) (*persistencespb.ChasmNode, error)

		// ChasmNodeToBlobs returns the metadata blob first, followed by the data blob.
		ChasmNodeToBlobs(node *persistencespb.ChasmNode) (*commonpb.DataBlob, *commonpb.DataBlob, error)
		ChasmNodeFromBlobs(metadata *commonpb.DataBlob, data *commonpb.DataBlob) (*persistencespb.ChasmNode, error)
	}

	// SerializationError is an error type for serialization
	SerializationError struct {
		encodingType enumspb.EncodingType
		wrappedErr   error
	}

	// DeserializationError is an error type for deserialization
	DeserializationError struct {
		encodingType enumspb.EncodingType
		wrappedErr   error
	}

	// UnknownEncodingTypeError is an error type for unknown or unsupported encoding type
	UnknownEncodingTypeError struct {
		providedType        string
		expectedEncodingStr []string
	}

	serializerImpl struct {
		TaskSerializer
		codec Codec
	}

	marshaler interface {
		Marshal() ([]byte, error)
	}
)

// NewSerializer returns a Serializer using Proto3 encoding.
func NewSerializer() Serializer {
	return NewSerializerWithCodec(NewProto3Codec())
}

// NewSerializerWithCodec returns a Serializer using the given codec.
func NewSerializerWithCodec(c Codec) Serializer {
	return &serializerImpl{codec: c}
}

func (s *serializerImpl) SerializeEvents(events []*historypb.HistoryEvent) (*commonpb.DataBlob, error) {
	return s.codec.Encode(&historypb.History{Events: events})
}

func (s *serializerImpl) DeserializeEvents(data *commonpb.DataBlob) ([]*historypb.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	events := &historypb.History{}
	err := s.codec.Decode(data, events)
	if err != nil {
		return nil, NewDeserializationError(s.codec.EncodingType(), err)
	}
	return events.Events, nil
}

func (s *serializerImpl) DeserializeStrippedEvents(data *commonpb.DataBlob) ([]*historyspb.StrippedHistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	events := &historyspb.StrippedHistoryEvents{}
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Discard unknown fields to improve performance. StrippedHistoryEvents is usually deserialized from HistoryEvent
		// which has extra fields that are not needed for this message.
		err := proto.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data.Data, events)
		if err != nil {
			return nil, NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
		}
	case enumspb.ENCODING_TYPE_JSON:
		// Discard unknown fields here, too.
		err := protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data.Data, events)
		if err != nil {
			return nil, NewDeserializationError(enumspb.ENCODING_TYPE_JSON, err)
		}
	default:
		return nil, NewUnknownEncodingTypeError(data.EncodingType.String(),
			enumspb.ENCODING_TYPE_PROTO3, enumspb.ENCODING_TYPE_JSON)
	}
	return events.Events, nil
}

func (s *serializerImpl) SerializeEvent(event *historypb.HistoryEvent) (*commonpb.DataBlob, error) {
	if event == nil {
		return nil, nil
	}
	return s.codec.Encode(event)
}

func (s *serializerImpl) DeserializeEvent(data *commonpb.DataBlob) (*historypb.HistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	event := &historypb.HistoryEvent{}
	err := s.codec.Decode(data, event)
	if err != nil {
		return nil, NewDeserializationError(s.codec.EncodingType(), err)
	}

	return event, err
}

func (s *serializerImpl) SerializeClusterMetadata(cm *persistencespb.ClusterMetadata) (*commonpb.DataBlob, error) {
	if cm == nil {
		cm = &persistencespb.ClusterMetadata{}
	}
	return s.codec.Encode(cm)
}

func (s *serializerImpl) DeserializeClusterMetadata(data *commonpb.DataBlob) (*persistencespb.ClusterMetadata, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	cm := &persistencespb.ClusterMetadata{}
	err := s.codec.Decode(data, cm)
	if err != nil {
		return nil, NewSerializationError(s.codec.EncodingType(), err)
	}

	return cm, err
}

// NewUnknownEncodingTypeError returns a new instance of encoding type error
func NewUnknownEncodingTypeError(
	providedType string,
	expectedEncoding ...enumspb.EncodingType,
) error {
	if len(expectedEncoding) == 0 {
		for encodingType := range enumspb.EncodingType_name {
			expectedEncoding = append(expectedEncoding, enumspb.EncodingType(encodingType))
		}
	}
	expectedEncodingStr := make([]string, 0, len(expectedEncoding))
	for _, encodingType := range expectedEncoding {
		expectedEncodingStr = append(expectedEncodingStr, encodingType.String())
	}
	return &UnknownEncodingTypeError{
		providedType:        providedType,
		expectedEncodingStr: expectedEncodingStr,
	}
}

func (e *UnknownEncodingTypeError) Error() string {
	return fmt.Sprintf("unknown or unsupported encoding type %v, supported types: %v",
		e.providedType,
		strings.Join(e.expectedEncodingStr, ","),
	)
}

// IsTerminalTaskError informs our task processing subsystem that it is impossible
// to retry this error
func (e *UnknownEncodingTypeError) IsTerminalTaskError() bool { return true }

// NewSerializationError returns a SerializationError
func NewSerializationError(
	encodingType enumspb.EncodingType,
	serializationErr error,
) error {
	return &SerializationError{
		encodingType: encodingType,
		wrappedErr:   serializationErr,
	}
}

func (e *SerializationError) Error() string {
	return fmt.Sprintf("error serializing using %v encoding: %v", e.encodingType, e.wrappedErr)
}

func (e *SerializationError) Unwrap() error {
	return e.wrappedErr
}

// NewDeserializationError returns a DeserializationError
func NewDeserializationError(
	encodingType enumspb.EncodingType,
	deserializationErr error,
) error {
	return &DeserializationError{
		encodingType: encodingType,
		wrappedErr:   deserializationErr,
	}
}

func (e *DeserializationError) Error() string {
	return fmt.Sprintf("error deserializing using %v encoding: %v", e.encodingType, e.wrappedErr)
}

func (e *DeserializationError) Unwrap() error {
	return e.wrappedErr
}

// IsTerminalTaskError informs our task processing subsystem that it is impossible to
// retry this error and that the task should be sent to a DLQ
func (e *DeserializationError) IsTerminalTaskError() bool { return true }

func (s *serializerImpl) ShardInfoToBlob(info *persistencespb.ShardInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) ShardInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ShardInfo, error) {
	shardInfo := &persistencespb.ShardInfo{}
	err := s.codec.Decode(data, shardInfo)

	if err != nil {
		return nil, err
	}

	if shardInfo.GetReplicationDlqAckLevel() == nil {
		shardInfo.ReplicationDlqAckLevel = make(map[string]int64)
	}

	if shardInfo.GetQueueStates() == nil {
		shardInfo.QueueStates = make(map[int32]*persistencespb.QueueState)
	}
	for _, queueState := range shardInfo.QueueStates {
		if queueState.ReaderStates == nil {
			queueState.ReaderStates = make(map[int64]*persistencespb.QueueReaderState)
		}
		for _, readerState := range queueState.ReaderStates {
			if readerState.Scopes == nil {
				readerState.Scopes = make([]*persistencespb.QueueSliceScope, 0)
			}
		}
	}

	return shardInfo, nil
}

func (s *serializerImpl) NamespaceDetailToBlob(info *persistencespb.NamespaceDetail) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) NamespaceDetailFromBlob(data *commonpb.DataBlob) (*persistencespb.NamespaceDetail, error) {
	result := &persistencespb.NamespaceDetail{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) HistoryTreeInfoToBlob(info *persistencespb.HistoryTreeInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) HistoryTreeInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryTreeInfo, error) {
	result := &persistencespb.HistoryTreeInfo{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) HistoryBranchToBlob(info *persistencespb.HistoryBranch) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) HistoryBranchFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryBranch, error) {
	result := &persistencespb.HistoryBranch{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) WorkflowExecutionInfoToBlob(info *persistencespb.WorkflowExecutionInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) WorkflowExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionInfo, error) {
	result := &persistencespb.WorkflowExecutionInfo{}
	err := s.codec.Decode(data, result)
	if err != nil {
		return nil, err
	}
	// Proto serialization replaces empty maps with nils, ensure this map is never nil.
	if result.SubStateMachinesByType == nil {
		result.SubStateMachinesByType = make(map[string]*persistencespb.StateMachineMap)
	}
	return result, nil
}

func (s *serializerImpl) WorkflowExecutionStateToBlob(info *persistencespb.WorkflowExecutionState) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) WorkflowExecutionStateFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error) {
	return WorkflowExecutionStateFromBlob(data.GetData(), data.GetEncodingType().String())
}

func (s *serializerImpl) ActivityInfoToBlob(info *persistencespb.ActivityInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) ActivityInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ActivityInfo, error) {
	result := &persistencespb.ActivityInfo{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) ChildExecutionInfoToBlob(info *persistencespb.ChildExecutionInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) ChildExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ChildExecutionInfo, error) {
	result := &persistencespb.ChildExecutionInfo{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) SignalInfoToBlob(info *persistencespb.SignalInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) SignalInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.SignalInfo, error) {
	result := &persistencespb.SignalInfo{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) RequestCancelInfoToBlob(info *persistencespb.RequestCancelInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) RequestCancelInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.RequestCancelInfo, error) {
	result := &persistencespb.RequestCancelInfo{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) TimerInfoToBlob(info *persistencespb.TimerInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) TimerInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerInfo, error) {
	result := &persistencespb.TimerInfo{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) TaskInfoToBlob(info *persistencespb.AllocatedTaskInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) TaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.AllocatedTaskInfo, error) {
	result := &persistencespb.AllocatedTaskInfo{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) TaskQueueInfoToBlob(info *persistencespb.TaskQueueInfo) (*commonpb.DataBlob, error) {
	return s.codec.Encode(info)
}

func (s *serializerImpl) TaskQueueInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueInfo, error) {
	result := &persistencespb.TaskQueueInfo{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) TaskQueueUserDataToBlob(data *persistencespb.TaskQueueUserData) (*commonpb.DataBlob, error) {
	return s.codec.Encode(data)
}

func (s *serializerImpl) TaskQueueUserDataFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueUserData, error) {
	result := &persistencespb.TaskQueueUserData{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) ChecksumToBlob(checksum *persistencespb.Checksum) (*commonpb.DataBlob, error) {
	// nil is replaced with empty object because it is not supported for "checksum" field in DB.
	if checksum == nil {
		checksum = &persistencespb.Checksum{}
	}
	return s.codec.Encode(checksum)
}

func (s *serializerImpl) ChecksumFromBlob(data *commonpb.DataBlob) (*persistencespb.Checksum, error) {
	result := &persistencespb.Checksum{}
	err := s.codec.Decode(data, result)
	if err != nil || result.GetFlavor() == enumsspb.CHECKSUM_FLAVOR_UNSPECIFIED {
		// If result is an empty struct (Flavor is unspecified), replace it with nil, because everywhere in the code checksum is pointer type.
		return nil, err
	}
	return result, nil
}

func (s *serializerImpl) QueueMetadataToBlob(metadata *persistencespb.QueueMetadata) (*commonpb.DataBlob, error) {
	return s.codec.Encode(metadata)
}

func (s *serializerImpl) QueueMetadataFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueMetadata, error) {
	result := &persistencespb.QueueMetadata{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) ReplicationTaskToBlob(replicationTask *replicationspb.ReplicationTask) (*commonpb.DataBlob, error) {
	return s.codec.Encode(replicationTask)
}

func (s *serializerImpl) ReplicationTaskFromBlob(data *commonpb.DataBlob) (*replicationspb.ReplicationTask, error) {
	result := &replicationspb.ReplicationTask{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) NexusEndpointToBlob(endpoint *persistencespb.NexusEndpoint) (*commonpb.DataBlob, error) {
	return s.codec.Encode(endpoint)
}

func (s *serializerImpl) NexusEndpointFromBlob(data *commonpb.DataBlob) (*persistencespb.NexusEndpoint, error) {
	result := &persistencespb.NexusEndpoint{}
	return result, s.codec.Decode(data, result)
}

func (s *serializerImpl) ChasmNodeToBlobs(node *persistencespb.ChasmNode) (metadata *commonpb.DataBlob, nodeData *commonpb.DataBlob, err error) {
	metadata, err = s.codec.Encode(node.Metadata)
	if err != nil {
		return nil, nil, err
	}
	return metadata, node.Data, nil
}

func (s *serializerImpl) ChasmNodeFromBlobs(metadata *commonpb.DataBlob, data *commonpb.DataBlob) (*persistencespb.ChasmNode, error) {
	result := &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{},
		Data:     data,
	}

	return result, s.codec.Decode(metadata, result.Metadata)
}

func (s *serializerImpl) ChasmNodeToBlob(node *persistencespb.ChasmNode) (*commonpb.DataBlob, error) {
	return s.codec.Encode(node)
}

func (s *serializerImpl) ChasmNodeFromBlob(blob *commonpb.DataBlob) (*persistencespb.ChasmNode, error) {
	result := &persistencespb.ChasmNode{}
	return result, s.codec.Decode(blob, result)
}
