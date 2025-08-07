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
	}

	marshaler interface {
		Marshal() ([]byte, error)
	}
)

// NewSerializer returns a PayloadSerializer
func NewSerializer() Serializer {
	return &serializerImpl{}
}

func (t *serializerImpl) SerializeEvents(events []*historypb.HistoryEvent) (*commonpb.DataBlob, error) {
	return t.serialize(&historypb.History{Events: events})
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
		return nil, NewUnknownEncodingTypeError(data.EncodingType.String(), enumspb.ENCODING_TYPE_PROTO3)
	}
	if err != nil {
		return nil, NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return events.Events, nil
}

func (t *serializerImpl) DeserializeStrippedEvents(data *commonpb.DataBlob) ([]*historyspb.StrippedHistoryEvent, error) {
	if data == nil {
		return nil, nil
	}
	if len(data.Data) == 0 {
		return nil, nil
	}

	events := &historyspb.StrippedHistoryEvents{}
	var err error
	//nolint:exhaustive
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Discard unknown fields to improve performance. StrippedHistoryEvents is usually deserialized from HistoryEvent
		// which has extra fields that are not needed for this message.
		err = proto.UnmarshalOptions{
			DiscardUnknown: true,
		}.Unmarshal(data.Data, events)
	default:
		return nil, NewUnknownEncodingTypeError(data.EncodingType.String(), enumspb.ENCODING_TYPE_PROTO3)
	}
	if err != nil {
		return nil, NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return events.Events, nil
}

func (t *serializerImpl) SerializeEvent(event *historypb.HistoryEvent) (*commonpb.DataBlob, error) {
	if event == nil {
		return nil, nil
	}
	return t.serialize(event)
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
		return nil, NewUnknownEncodingTypeError(data.EncodingType.String(), enumspb.ENCODING_TYPE_PROTO3)
	}
	if err != nil {
		return nil, NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}

	return event, err
}

func (t *serializerImpl) SerializeClusterMetadata(cm *persistencespb.ClusterMetadata) (*commonpb.DataBlob, error) {
	if cm == nil {
		cm = &persistencespb.ClusterMetadata{}
	}
	return t.serialize(cm)
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
		return nil, NewUnknownEncodingTypeError(data.EncodingType.String(), enumspb.ENCODING_TYPE_PROTO3)
	}
	if err != nil {
		return nil, NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}

	return cm, err
}

func (t *serializerImpl) serialize(p marshaler) (*commonpb.DataBlob, error) {
	if p == nil {
		return nil, nil
	}

	data, err := p.Marshal()
	if err != nil {
		return nil, NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}

	// Shouldn't happen, but keeping
	if data == nil {
		return nil, nil
	}

	return &commonpb.DataBlob{
		Data:         data,
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	}, nil
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

func (t *serializerImpl) ShardInfoToBlob(info *persistencespb.ShardInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) ShardInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ShardInfo, error) {
	shardInfo := &persistencespb.ShardInfo{}
	err := Decode(data, shardInfo)

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

func (t *serializerImpl) NamespaceDetailToBlob(info *persistencespb.NamespaceDetail) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) NamespaceDetailFromBlob(data *commonpb.DataBlob) (*persistencespb.NamespaceDetail, error) {
	result := &persistencespb.NamespaceDetail{}
	return result, Decode(data, result)
}

func (t *serializerImpl) HistoryTreeInfoToBlob(info *persistencespb.HistoryTreeInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) HistoryTreeInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryTreeInfo, error) {
	result := &persistencespb.HistoryTreeInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) HistoryBranchToBlob(info *persistencespb.HistoryBranch) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) HistoryBranchFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryBranch, error) {
	result := &persistencespb.HistoryBranch{}
	return result, Decode(data, result)
}

func (t *serializerImpl) WorkflowExecutionInfoToBlob(info *persistencespb.WorkflowExecutionInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) WorkflowExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionInfo, error) {
	result := &persistencespb.WorkflowExecutionInfo{}
	err := Decode(data, result)
	if err != nil {
		return nil, err
	}
	// Proto serialization replaces empty maps with nils, ensure this map is never nil.
	if result.SubStateMachinesByType == nil {
		result.SubStateMachinesByType = make(map[string]*persistencespb.StateMachineMap)
	}
	return result, nil
}

func (t *serializerImpl) WorkflowExecutionStateToBlob(info *persistencespb.WorkflowExecutionState) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) WorkflowExecutionStateFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error) {
	return WorkflowExecutionStateFromBlob(data)
}

func (t *serializerImpl) ActivityInfoToBlob(info *persistencespb.ActivityInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) ActivityInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ActivityInfo, error) {
	result := &persistencespb.ActivityInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) ChildExecutionInfoToBlob(info *persistencespb.ChildExecutionInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) ChildExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ChildExecutionInfo, error) {
	result := &persistencespb.ChildExecutionInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) SignalInfoToBlob(info *persistencespb.SignalInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) SignalInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.SignalInfo, error) {
	result := &persistencespb.SignalInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) RequestCancelInfoToBlob(info *persistencespb.RequestCancelInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) RequestCancelInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.RequestCancelInfo, error) {
	result := &persistencespb.RequestCancelInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) TimerInfoToBlob(info *persistencespb.TimerInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) TimerInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerInfo, error) {
	result := &persistencespb.TimerInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) TaskInfoToBlob(info *persistencespb.AllocatedTaskInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) TaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.AllocatedTaskInfo, error) {
	result := &persistencespb.AllocatedTaskInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) TaskQueueInfoToBlob(info *persistencespb.TaskQueueInfo) (*commonpb.DataBlob, error) {
	return ProtoEncode(info)
}

func (t *serializerImpl) TaskQueueInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueInfo, error) {
	result := &persistencespb.TaskQueueInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) TaskQueueUserDataToBlob(data *persistencespb.TaskQueueUserData) (*commonpb.DataBlob, error) {
	return ProtoEncode(data)
}

func (t *serializerImpl) TaskQueueUserDataFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueUserData, error) {
	result := &persistencespb.TaskQueueUserData{}
	return result, Decode(data, result)
}

func (t *serializerImpl) ChecksumToBlob(checksum *persistencespb.Checksum) (*commonpb.DataBlob, error) {
	// nil is replaced with empty object because it is not supported for "checksum" field in DB.
	if checksum == nil {
		checksum = &persistencespb.Checksum{}
	}
	return ProtoEncode(checksum)
}

func (t *serializerImpl) ChecksumFromBlob(data *commonpb.DataBlob) (*persistencespb.Checksum, error) {
	result := &persistencespb.Checksum{}
	err := Decode(data, result)
	if err != nil || result.GetFlavor() == enumsspb.CHECKSUM_FLAVOR_UNSPECIFIED {
		// If result is an empty struct (Flavor is unspecified), replace it with nil, because everywhere in the code checksum is pointer type.
		return nil, err
	}
	return result, nil
}

func (t *serializerImpl) QueueMetadataToBlob(metadata *persistencespb.QueueMetadata) (*commonpb.DataBlob, error) {
	return ProtoEncode(metadata)
}

func (t *serializerImpl) QueueMetadataFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueMetadata, error) {
	result := &persistencespb.QueueMetadata{}
	return result, Decode(data, result)
}

func (t *serializerImpl) ReplicationTaskToBlob(replicationTask *replicationspb.ReplicationTask) (*commonpb.DataBlob, error) {
	return ProtoEncode(replicationTask)
}

func (t *serializerImpl) ReplicationTaskFromBlob(data *commonpb.DataBlob) (*replicationspb.ReplicationTask, error) {
	result := &replicationspb.ReplicationTask{}
	return result, Decode(data, result)
}

func (t *serializerImpl) NexusEndpointToBlob(endpoint *persistencespb.NexusEndpoint) (*commonpb.DataBlob, error) {
	return ProtoEncode(endpoint)
}

func (t *serializerImpl) NexusEndpointFromBlob(data *commonpb.DataBlob) (*persistencespb.NexusEndpoint, error) {
	result := &persistencespb.NexusEndpoint{}
	return result, Decode(data, result)
}

func (t *serializerImpl) ChasmNodeToBlobs(node *persistencespb.ChasmNode) (metadata *commonpb.DataBlob, nodedata *commonpb.DataBlob, retErr error) {
	metadata, retErr = ProtoEncode(node.Metadata)
	if retErr != nil {
		return nil, nil, retErr
	}
	return metadata, node.Data, nil
}

func (t *serializerImpl) ChasmNodeFromBlobs(metadata *commonpb.DataBlob, data *commonpb.DataBlob) (*persistencespb.ChasmNode, error) {
	result := &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{},
		Data:     data,
	}
	return result, Decode(metadata, result.Metadata)
}

func (t *serializerImpl) ChasmNodeToBlob(node *persistencespb.ChasmNode) (*commonpb.DataBlob, error) {
	return ProtoEncode(node)
}

func (t *serializerImpl) ChasmNodeFromBlob(blob *commonpb.DataBlob) (*persistencespb.ChasmNode, error) {
	result := &persistencespb.ChasmNode{}
	return result, Decode(blob, result)
}
