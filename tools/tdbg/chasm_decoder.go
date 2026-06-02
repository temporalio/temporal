package tdbg

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
)

type decodedTask struct {
	TypeID              uint32                              `json:"typeId"`
	TaskFQN             string                              `json:"taskFQN,omitempty"`
	Destination         string                              `json:"destination,omitempty"`
	ScheduledTime       string                              `json:"scheduledTime,omitempty"`
	DecodedData         json.RawMessage                     `json:"decodedData,omitempty"`
	RawData             *commonpb.DataBlob                  `json:"rawData,omitempty"`
	VersionedTransition *persistencespb.VersionedTransition `json:"versionedTransition,omitempty"`
	PhysicalTaskStatus  int32                               `json:"physicalTaskStatus,omitempty"`
}

type decodedChasmNode struct {
	Metadata        *persistencespb.ChasmNodeMetadata `json:"metadata"`
	DecodedData     json.RawMessage                   `json:"decodedData,omitempty"`
	RawData         *commonpb.DataBlob                `json:"rawData,omitempty"`
	NodeType        string                            `json:"nodeType"`
	ComponentFQN    string                            `json:"componentFQN,omitempty"`
	SideEffectTasks []*decodedTask                    `json:"sideEffectTasks,omitempty"`
	PureTasks       []*decodedTask                    `json:"pureTasks,omitempty"`
}

func getNodeType(metadata *persistencespb.ChasmNodeMetadata) string {
	switch {
	case metadata.GetComponentAttributes() != nil:
		return "component"
	case metadata.GetCollectionAttributes() != nil:
		return "collection"
	case metadata.GetDataAttributes() != nil:
		return "data"
	case metadata.GetPointerAttributes() != nil:
		return "pointer"
	default:
		return "unknown"
	}
}

func decodeTask(
	task *persistencespb.ChasmComponentAttributes_Task,
	registry *chasm.Registry,
) (*decodedTask, error) {
	typeID := task.GetTypeId()
	fqn, _ := registry.TaskFqnByID(typeID)

	var scheduledTime string
	if ts := task.GetScheduledTime(); ts != nil {
		scheduledTime = ts.AsTime().UTC().Format(defaultDateTimeFormat)
	}

	decoded := &decodedTask{
		TypeID:              typeID,
		TaskFQN:             fqn,
		Destination:         task.GetDestination(),
		ScheduledTime:       scheduledTime,
		VersionedTransition: task.GetVersionedTransition(),
		PhysicalTaskStatus:  task.GetPhysicalTaskStatus(),
	}

	rt, ok := registry.TaskByID(typeID)
	if !ok {
		decoded.RawData = task.GetData()
		return decoded, nil
	}

	goType := rt.GoType()
	if goType == nil {
		decoded.RawData = task.GetData()
		return decoded, nil
	}

	messageValue := reflect.New(goType.Elem())
	message, ok := messageValue.Interface().(proto.Message)
	if !ok {
		decoded.RawData = task.GetData()
		return decoded, nil
	}

	dataBlob := task.GetData()
	if dataBlob != nil && len(dataBlob.GetData()) > 0 {
		message = message.ProtoReflect().New().Interface()
		if err := serialization.Decode(dataBlob, message); err != nil {
			decoded.RawData = dataBlob
			return decoded, nil
		}
	}

	jsonBytes, err := codec.NewJSONPBEncoder().Encode(message)
	if err != nil {
		decoded.RawData = dataBlob
		return decoded, nil
	}

	decoded.DecodedData = json.RawMessage(jsonBytes)
	return decoded, nil
}

func decodeNode(node *persistencespb.ChasmNode, registry *chasm.Registry) (*decodedChasmNode, error) {
	metadata := node.GetMetadata()
	componentAttr := metadata.GetComponentAttributes()

	if componentAttr == nil {
		return &decodedChasmNode{
			Metadata: metadata,
			RawData:  node.GetData(),
			NodeType: getNodeType(metadata),
		}, nil
	}

	typeID := componentAttr.GetTypeId()
	rc, ok := registry.ComponentByID(typeID)
	if !ok {
		return &decodedChasmNode{
			Metadata: metadata,
			RawData:  node.GetData(),
			NodeType: "component (unknown)",
		}, nil
	}

	result := &decodedChasmNode{
		Metadata: metadata,
		NodeType: getNodeType(metadata),
	}

	goType := rc.GoType()
	if goType == nil {
		return nil, errors.New("component has no Go type")
	}

	messageValue := reflect.New(goType.Elem())
	message, ok := messageValue.Interface().(proto.Message)
	if !ok {
		result.RawData = node.GetData()
		result.NodeType = "component (stateless)"
	} else {
		dataBlob := node.GetData()
		if dataBlob != nil && len(dataBlob.GetData()) > 0 {
			message = message.ProtoReflect().New().Interface()
			if err := serialization.Decode(dataBlob, message); err != nil {
				result.RawData = dataBlob
				result.NodeType = "component (decode error)"
			} else {
				jsonBytes, err := codec.NewJSONPBEncoder().Encode(message)
				if err != nil {
					return nil, fmt.Errorf("failed to encode to JSON: %w", err)
				}
				result.DecodedData = json.RawMessage(jsonBytes)
				result.NodeType = "component"
			}
		}
	}

	fqn, _ := registry.ComponentFqnByID(typeID)
	result.ComponentFQN = fqn

	sideEffectTasks, err := decodeTasks(componentAttr.GetSideEffectTasks(), registry)
	if err != nil {
		return nil, fmt.Errorf("failed to decode side effect task: %w", err)
	}
	result.SideEffectTasks = sideEffectTasks

	pureTasks, err := decodeTasks(componentAttr.GetPureTasks(), registry)
	if err != nil {
		return nil, fmt.Errorf("failed to decode pure task: %w", err)
	}
	result.PureTasks = pureTasks

	return result, nil
}

func decodeTasks(
	tasks []*persistencespb.ChasmComponentAttributes_Task,
	registry *chasm.Registry,
) ([]*decodedTask, error) {
	result := make([]*decodedTask, len(tasks))
	for i, task := range tasks {
		decodedTask, err := decodeTask(task, registry)
		if err != nil {
			return nil, err
		}
		result[i] = decodedTask
	}
	return result, nil
}

func decodeChasmNodes(
	chasmNodes map[string]*persistencespb.ChasmNode,
	registry *chasm.Registry,
) (map[string]*decodedChasmNode, error) {
	decoded := make(map[string]*decodedChasmNode, len(chasmNodes))
	for path, node := range chasmNodes {
		decodedNode, err := decodeNode(node, registry)
		if err != nil {
			return nil, fmt.Errorf("failed to decode node at path %q: %w", path, err)
		}
		decoded[path] = decodedNode
	}
	return decoded, nil
}
