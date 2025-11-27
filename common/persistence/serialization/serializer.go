package serialization

import (
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalproto"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

// DefaultDecoder is here for convenience to skip the need to create a new Serializer when only decodig is needed.
// It does not need an encoding type; as it will use the one defined in the DataBlob.
var r Serializer = &serializerImpl{encodingType: enumspb.ENCODING_TYPE_UNSPECIFIED}
var DefaultDecoder Decoder = r

type (
	// Encoder is used to encode objects to DataBlobs.
	Encoder interface {
		SerializeEvents(batch []*historypb.HistoryEvent) (*commonpb.DataBlob, error)
		SerializeEvent(event *historypb.HistoryEvent) (*commonpb.DataBlob, error)
		SerializeClusterMetadata(icm *persistencespb.ClusterMetadata) (*commonpb.DataBlob, error)
		ShardInfoToBlob(info *persistencespb.ShardInfo) (*commonpb.DataBlob, error)
		NamespaceDetailToBlob(info *persistencespb.NamespaceDetail) (*commonpb.DataBlob, error)
		HistoryTreeInfoToBlob(info *persistencespb.HistoryTreeInfo) (*commonpb.DataBlob, error)
		HistoryBranchToBlob(info *persistencespb.HistoryBranch) (*commonpb.DataBlob, error)
		WorkflowExecutionInfoToBlob(info *persistencespb.WorkflowExecutionInfo) (*commonpb.DataBlob, error)
		WorkflowExecutionStateToBlob(info *persistencespb.WorkflowExecutionState) (*commonpb.DataBlob, error)
		ActivityInfoToBlob(info *persistencespb.ActivityInfo) (*commonpb.DataBlob, error)
		ChildExecutionInfoToBlob(info *persistencespb.ChildExecutionInfo) (*commonpb.DataBlob, error)
		SignalInfoToBlob(info *persistencespb.SignalInfo) (*commonpb.DataBlob, error)
		RequestCancelInfoToBlob(info *persistencespb.RequestCancelInfo) (*commonpb.DataBlob, error)
		TimerInfoToBlob(info *persistencespb.TimerInfo) (*commonpb.DataBlob, error)
		TaskInfoToBlob(info *persistencespb.AllocatedTaskInfo) (*commonpb.DataBlob, error)
		TaskQueueInfoToBlob(info *persistencespb.TaskQueueInfo) (*commonpb.DataBlob, error)
		TaskQueueUserDataToBlob(info *persistencespb.TaskQueueUserData) (*commonpb.DataBlob, error)
		ChecksumToBlob(checksum *persistencespb.Checksum) (*commonpb.DataBlob, error)
		QueueMetadataToBlob(metadata *persistencespb.QueueMetadata) (*commonpb.DataBlob, error)
		ReplicationTaskToBlob(replicationTask *replicationspb.ReplicationTask) (*commonpb.DataBlob, error)
		NexusEndpointToBlob(endpoint *persistencespb.NexusEndpoint) (*commonpb.DataBlob, error)
		// ChasmNodeToBlob returns a single encoded blob for the node.
		ChasmNodeToBlob(node *persistencespb.ChasmNode) (*commonpb.DataBlob, error)
		// ChasmNodeToBlobs returns the metadata blob first, followed by the data blob.
		ChasmNodeToBlobs(node *persistencespb.ChasmNode) (*commonpb.DataBlob, *commonpb.DataBlob, error)
		TransferTaskInfoToBlob(info *persistencespb.TransferTaskInfo) (*commonpb.DataBlob, error)
		TimerTaskInfoToBlob(info *persistencespb.TimerTaskInfo) (*commonpb.DataBlob, error)
		ReplicationTaskInfoToBlob(info *persistencespb.ReplicationTaskInfo) (*commonpb.DataBlob, error)
		VisibilityTaskInfoToBlob(info *persistencespb.VisibilityTaskInfo) (*commonpb.DataBlob, error)
		ArchivalTaskInfoToBlob(info *persistencespb.ArchivalTaskInfo) (*commonpb.DataBlob, error)
		OutboundTaskInfoToBlob(info *persistencespb.OutboundTaskInfo) (*commonpb.DataBlob, error)
		QueueStateToBlob(info *persistencespb.QueueState) (*commonpb.DataBlob, error)
		SerializeTask(task tasks.Task) (*commonpb.DataBlob, error)
		SerializeReplicationTask(task tasks.Task) (*persistencespb.ReplicationTaskInfo, error)
	}

	// Decoder is used to decode DataBlobs to objects.
	Decoder interface {
		DeserializeEvents(data *commonpb.DataBlob) ([]*historypb.HistoryEvent, error)
		DeserializeEvent(data *commonpb.DataBlob) (*historypb.HistoryEvent, error)
		DeserializeStrippedEvents(data *commonpb.DataBlob) ([]*historyspb.StrippedHistoryEvent, error)
		DeserializeClusterMetadata(data *commonpb.DataBlob) (*persistencespb.ClusterMetadata, error)
		ShardInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ShardInfo, error)
		NamespaceDetailFromBlob(data *commonpb.DataBlob) (*persistencespb.NamespaceDetail, error)
		HistoryTreeInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryTreeInfo, error)
		HistoryBranchFromBlob(data []byte) (*persistencespb.HistoryBranch, error)
		WorkflowExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionInfo, error)
		WorkflowExecutionStateFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error)
		ActivityInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ActivityInfo, error)
		ChildExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ChildExecutionInfo, error)
		SignalInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.SignalInfo, error)
		RequestCancelInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.RequestCancelInfo, error)
		TimerInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerInfo, error)
		TaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.AllocatedTaskInfo, error)
		TaskQueueInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueInfo, error)
		TaskQueueUserDataFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueUserData, error)
		ChecksumFromBlob(data *commonpb.DataBlob) (*persistencespb.Checksum, error)
		QueueMetadataFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueMetadata, error)
		ReplicationTaskFromBlob(data *commonpb.DataBlob) (*replicationspb.ReplicationTask, error)
		NexusEndpointFromBlob(data *commonpb.DataBlob) (*persistencespb.NexusEndpoint, error)
		ChasmNodeFromBlob(blob *commonpb.DataBlob) (*persistencespb.ChasmNode, error)
		ChasmNodeFromBlobs(metadata *commonpb.DataBlob, data *commonpb.DataBlob) (*persistencespb.ChasmNode, error)
		TransferTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TransferTaskInfo, error)
		TimerTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerTaskInfo, error)
		ReplicationTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ReplicationTaskInfo, error)
		VisibilityTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.VisibilityTaskInfo, error)
		ArchivalTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ArchivalTaskInfo, error)
		OutboundTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.OutboundTaskInfo, error)
		QueueStateFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueState, error)
		DeserializeTask(category tasks.Category, blob *commonpb.DataBlob) (tasks.Task, error)
		DeserializeReplicationTask(replicationTask *persistencespb.ReplicationTaskInfo) (tasks.Task, error)
	}

	// Serializer is used to serialize and deserialize DataBlobs.
	Serializer interface {
		Encoder
		Decoder
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
		encodingType enumspb.EncodingType
	}

	marshaler interface {
		Marshal() ([]byte, error)
	}
)

func NewSerializer() Serializer {
	return &serializerImpl{encodingType: enumspb.ENCODING_TYPE_PROTO3}
}

func (t *serializerImpl) SerializeTask(
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	category := task.GetCategory()
	switch category.ID() {
	case tasks.CategoryIDTransfer:
		return serializeTransferTask(t, task)
	case tasks.CategoryIDTimer:
		return serializeTimerTask(t, task)
	case tasks.CategoryIDVisibility:
		return serializeVisibilityTask(t, task)
	case tasks.CategoryIDReplication:
		return serializeReplicationTask(t, task)
	case tasks.CategoryIDArchival:
		return serializeArchivalTask(t, task)
	case tasks.CategoryIDOutbound:
		return serializeOutboundTask(t, task)
	default:
		return nil, serviceerror.NewInternalf("Unknown task category: %v", category)
	}
}

func (t *serializerImpl) DeserializeTask(
	category tasks.Category,
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	switch category.ID() {
	case tasks.CategoryIDTransfer:
		return deserializeTransferTask(t, blob)
	case tasks.CategoryIDTimer:
		return deserializeTimerTask(t, blob)
	case tasks.CategoryIDVisibility:
		return deserializeVisibilityTask(t, blob)
	case tasks.CategoryIDReplication:
		return deserializeReplicationTask(t, blob)
	case tasks.CategoryIDArchival:
		return deserializeArchivalTask(t, blob)
	case tasks.CategoryIDOutbound:
		return deserializeOutboundTask(t, blob)
	default:
		return nil, serviceerror.NewInternalf("Unknown task category: %v", category)
	}
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
	err := Decode(data, events)
	if err != nil {
		return nil, err
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
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		// Discard unknown fields to improve performance. StrippedHistoryEvents is usually deserialized from HistoryEvent
		// which has extra fields that are not needed for this message.
		err = proto.UnmarshalOptions{
			DiscardUnknown: true,
		}.Unmarshal(data.Data, events)
	case enumspb.ENCODING_TYPE_JSON:
		err = temporalproto.CustomJSONUnmarshalOptions{
			DiscardUnknown: true,
		}.Unmarshal(data.Data, events)
	default:
		return nil, NewUnknownEncodingTypeError(data.EncodingType.String(),
			enumspb.ENCODING_TYPE_PROTO3, enumspb.ENCODING_TYPE_JSON)
	}
	if err != nil {
		return nil, NewDeserializationError(data.EncodingType, err)
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
	err := Decode(data, event)
	if err != nil {
		return nil, err
	}
	return event, nil
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
	err := Decode(data, cm)
	if err != nil {
		return nil, err
	}
	return cm, nil
}

func (t *serializerImpl) serialize(p proto.Message) (*commonpb.DataBlob, error) {
	if p == nil {
		return nil, nil
	}
	blob, err := encodeBlob(p, t.encodingType)
	if err != nil {
		return nil, NewSerializationError(t.encodingType, err)
	}
	return blob, nil
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
	return encodeBlob(info, t.encodingType)
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
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) NamespaceDetailFromBlob(data *commonpb.DataBlob) (*persistencespb.NamespaceDetail, error) {
	result := &persistencespb.NamespaceDetail{}
	return result, Decode(data, result)
}

func (t *serializerImpl) HistoryTreeInfoToBlob(info *persistencespb.HistoryTreeInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) HistoryTreeInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.HistoryTreeInfo, error) {
	result := &persistencespb.HistoryTreeInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) HistoryBranchToBlob(info *persistencespb.HistoryBranch) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

// NOTE: HistoryBranch does not have an encoding type; so we use the serializer's encoding type.
func (t *serializerImpl) HistoryBranchFromBlob(data []byte) (*persistencespb.HistoryBranch, error) {
	result := &persistencespb.HistoryBranch{}
	return result, Decode(&commonpb.DataBlob{Data: data, EncodingType: t.encodingType}, result)
}

func (t *serializerImpl) WorkflowExecutionInfoToBlob(info *persistencespb.WorkflowExecutionInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
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
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) WorkflowExecutionStateFromBlob(data *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error) {
	result := &persistencespb.WorkflowExecutionState{}
	if err := Decode(data, result); err != nil {
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

func (t *serializerImpl) ActivityInfoToBlob(info *persistencespb.ActivityInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) ActivityInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ActivityInfo, error) {
	result := &persistencespb.ActivityInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) ChildExecutionInfoToBlob(info *persistencespb.ChildExecutionInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) ChildExecutionInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ChildExecutionInfo, error) {
	result := &persistencespb.ChildExecutionInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) SignalInfoToBlob(info *persistencespb.SignalInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) SignalInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.SignalInfo, error) {
	result := &persistencespb.SignalInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) RequestCancelInfoToBlob(info *persistencespb.RequestCancelInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) RequestCancelInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.RequestCancelInfo, error) {
	result := &persistencespb.RequestCancelInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) TimerInfoToBlob(info *persistencespb.TimerInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) TimerInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerInfo, error) {
	result := &persistencespb.TimerInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) TaskInfoToBlob(info *persistencespb.AllocatedTaskInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) TaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.AllocatedTaskInfo, error) {
	result := &persistencespb.AllocatedTaskInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) TaskQueueInfoToBlob(info *persistencespb.TaskQueueInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) TaskQueueInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TaskQueueInfo, error) {
	result := &persistencespb.TaskQueueInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) TaskQueueUserDataToBlob(data *persistencespb.TaskQueueUserData) (*commonpb.DataBlob, error) {
	return encodeBlob(data, t.encodingType)
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
	return encodeBlob(checksum, t.encodingType)
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
	// TODO change ENCODING_TYPE_JSON to ENCODING_TYPE_PROTO3
	return encodeBlob(metadata, enumspb.ENCODING_TYPE_JSON)
}

func (t *serializerImpl) QueueMetadataFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueMetadata, error) {
	result := &persistencespb.QueueMetadata{}
	return result, Decode(data, result)
}

func (t *serializerImpl) ReplicationTaskToBlob(replicationTask *replicationspb.ReplicationTask) (*commonpb.DataBlob, error) {
	return encodeBlob(replicationTask, t.encodingType)
}

func (t *serializerImpl) ReplicationTaskFromBlob(data *commonpb.DataBlob) (*replicationspb.ReplicationTask, error) {
	result := &replicationspb.ReplicationTask{}
	return result, Decode(data, result)
}

func (t *serializerImpl) NexusEndpointToBlob(endpoint *persistencespb.NexusEndpoint) (*commonpb.DataBlob, error) {
	return encodeBlob(endpoint, t.encodingType)
}

func (t *serializerImpl) NexusEndpointFromBlob(data *commonpb.DataBlob) (*persistencespb.NexusEndpoint, error) {
	result := &persistencespb.NexusEndpoint{}
	return result, Decode(data, result)
}

func (t *serializerImpl) ChasmNodeToBlobs(node *persistencespb.ChasmNode) (metadata *commonpb.DataBlob, nodedata *commonpb.DataBlob, retErr error) {
	metadata, retErr = encodeBlob(node.Metadata, t.encodingType)
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
	return encodeBlob(node, t.encodingType)
}

func (t *serializerImpl) ChasmNodeFromBlob(blob *commonpb.DataBlob) (*persistencespb.ChasmNode, error) {
	result := &persistencespb.ChasmNode{}
	return result, Decode(blob, result)
}

func (t *serializerImpl) TransferTaskInfoToBlob(info *persistencespb.TransferTaskInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) TransferTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TransferTaskInfo, error) {
	result := &persistencespb.TransferTaskInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) TimerTaskInfoToBlob(info *persistencespb.TimerTaskInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) TimerTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.TimerTaskInfo, error) {
	result := &persistencespb.TimerTaskInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) ReplicationTaskInfoToBlob(info *persistencespb.ReplicationTaskInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) ReplicationTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ReplicationTaskInfo, error) {
	result := &persistencespb.ReplicationTaskInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) VisibilityTaskInfoToBlob(info *persistencespb.VisibilityTaskInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) VisibilityTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.VisibilityTaskInfo, error) {
	result := &persistencespb.VisibilityTaskInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) ArchivalTaskInfoToBlob(info *persistencespb.ArchivalTaskInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) ArchivalTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.ArchivalTaskInfo, error) {
	result := &persistencespb.ArchivalTaskInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) OutboundTaskInfoToBlob(info *persistencespb.OutboundTaskInfo) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) OutboundTaskInfoFromBlob(data *commonpb.DataBlob) (*persistencespb.OutboundTaskInfo, error) {
	result := &persistencespb.OutboundTaskInfo{}
	return result, Decode(data, result)
}

func (t *serializerImpl) QueueStateToBlob(info *persistencespb.QueueState) (*commonpb.DataBlob, error) {
	return encodeBlob(info, t.encodingType)
}

func (t *serializerImpl) QueueStateFromBlob(data *commonpb.DataBlob) (*persistencespb.QueueState, error) {
	result := &persistencespb.QueueState{}
	return result, Decode(data, result)
}
