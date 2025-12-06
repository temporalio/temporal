// TODO: move this to chasm_test package
package chasm

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/proto"
)

type (
	// TestComponent is a sample CHASM component used in tests.
	// It would be nice to move it another package, but this creates a circular dependency.

	protoMessageType = persistencespb.WorkflowExecutionState // Random proto message.
	TestComponent    struct {
		UnimplementedComponent

		ComponentData                *protoMessageType
		SubComponent1                Field[*TestSubComponent1]
		SubComponent2                Field[*TestSubComponent2]
		SubData1                     Field[*protoMessageType]
		SubComponents                Map[string, *TestSubComponent1]
		PendingActivities            Map[int, *TestSubComponent1]
		SubComponent11Pointer        Field[*TestSubComponent11]
		SubComponent11Pointer2       Field[*TestSubComponent11]
		SubComponentInterfacePointer Field[Component]

		MSPointer MSPointer
		ParentPtr ParentPtr[*TestComponent]

		Visibility Field[*Visibility]
	}

	TestSubComponent1 struct {
		UnimplementedComponent

		SubComponent1Data    *protoMessageType
		SubComponent11       Field[*TestSubComponent11]
		SubComponent11_2     Field[*TestSubComponent11]
		SubData11            Field[*protoMessageType] // Random proto message.
		SubComponent2Pointer Field[*TestSubComponent2]
		DataPointer          Field[*protoMessageType]

		ParentPtr ParentPtr[*TestComponent]
	}

	TestSubComponent11 struct {
		UnimplementedComponent

		SubComponent11Data *protoMessageType

		ParentPtr ParentPtr[*TestSubComponent1]
	}

	TestSubComponent2 struct {
		UnimplementedComponent
		SubComponent2Data *protoMessageType
	}

	TestSubComponent interface {
		GetData() string
	}
)

const (
	TestComponentStartTimeSAKey   = "StartTimeSAKey"
	TestComponentRunIDSAKey       = "RunIdSAKey"
	TestComponentStartTimeMemoKey = "StartTimeMemoKey"
)

var (
	TestComponentStartTimeSearchAttribute = NewSearchAttributeDateTime(TestComponentStartTimeSAKey, SearchAttributeFieldDateTime01)
	TestComponentRunIDPredefinedSA        = newSearchAttributeKeywordByField(TestComponentRunIDSAKey)

	_ VisibilitySearchAttributesProvider = (*TestComponent)(nil)
	_ VisibilityMemoProvider             = (*TestComponent)(nil)
)

func (tc *TestComponent) LifecycleState(_ Context) LifecycleState {
	switch tc.ComponentData.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return LifecycleStateRunning
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return LifecycleStateCompleted
	default:
		return LifecycleStateFailed
	}
}

func (tc *TestComponent) Terminate(
	mutableContext MutableContext,
	_ TerminateComponentRequest,
) (TerminateComponentResponse, error) {
	tc.Fail(mutableContext)
	return TerminateComponentResponse{}, nil
}

func (tc *TestComponent) Complete(_ MutableContext) {
	tc.ComponentData.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
}

func (tc *TestComponent) Fail(_ MutableContext) {
	tc.ComponentData.Status = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
}

// SearchAttributes implements VisibilitySearchAttributesProvider interface.
func (tc *TestComponent) SearchAttributes(_ Context) []SearchAttributeKeyValue {
	return []SearchAttributeKeyValue{
		TestComponentStartTimeSearchAttribute.Value(tc.ComponentData.GetStartTime().AsTime()),
		TestComponentRunIDPredefinedSA.Value(tc.ComponentData.GetRunId()),
		SearchAttributeTemporalScheduledByID.Value(tc.ComponentData.GetRunId()),
	}
}

// Memo implements VisibilityMemoProvider interface.
func (tc *TestComponent) Memo(_ Context) proto.Message {
	return tc.ComponentData
}

func (tsc1 *TestSubComponent1) LifecycleState(_ Context) LifecycleState {
	return LifecycleStateRunning
}

func (tsc1 *TestSubComponent1) GetData() string {
	return tsc1.SubComponent1Data.GetCreateRequestId()
}

func (tsc11 *TestSubComponent11) LifecycleState(_ Context) LifecycleState {
	return LifecycleStateRunning
}

func (tsc2 *TestSubComponent2) LifecycleState(_ Context) LifecycleState {
	return LifecycleStateRunning
}

func setTestComponentFields(c *TestComponent, backend *MockNodeBackend) {
	c.ComponentData = &protoMessageType{
		CreateRequestId: "component-data",
	}
	c.SubComponent1 = NewComponentField(nil, &TestSubComponent1{
		SubComponent1Data: &protoMessageType{
			CreateRequestId: "sub-component1-data",
		},
		SubComponent11: NewComponentField(nil, &TestSubComponent11{
			SubComponent11Data: &protoMessageType{
				CreateRequestId: "sub-component1-sub-component11-data",
			},
		}),
		SubData11: NewDataField(nil, &protoMessageType{
			CreateRequestId: "sub-component1-sub-data11",
		}),
	})
	c.SubComponent2 = NewEmptyField[*TestSubComponent2]()
	c.SubData1 = NewDataField(nil, &protoMessageType{
		CreateRequestId: "sub-data1",
	})
	c.MSPointer = NewMSPointer(backend)
}

// returns serialized version of TestComponent from above.
// Generated by generateMapInit function below.
func testComponentSerializedNodes() map[string]*persistencespb.ChasmNode {
	serializedNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          1,
				},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2,
					TransitionCount:          2,
				},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId:          testComponentTypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task(nil),
						PureTasks:       []*persistencespb.ChasmComponentAttributes_Task(nil),
					},
				},
			},
			Data: &commonpb.DataBlob{
				EncodingType: 1,
				Data:         []byte{0xa, 0xe, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x2d, 0x64, 0x61, 0x74, 0x61},
			},
		},
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          1,
				},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2,
					TransitionCount:          2,
				},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId:          testSubComponent1TypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task(nil),
						PureTasks:       []*persistencespb.ChasmComponentAttributes_Task(nil),
					},
				},
			},
			Data: &commonpb.DataBlob{
				EncodingType: 1,
				Data:         []byte{0xa, 0x13, 0x73, 0x75, 0x62, 0x2d, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x31, 0x2d, 0x64, 0x61, 0x74, 0x61},
			},
		},
		"SubComponent1/SubComponent11": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          1,
				},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2,
					TransitionCount:          2,
				},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId:          testSubComponent11TypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task(nil),
						PureTasks:       []*persistencespb.ChasmComponentAttributes_Task(nil),
					},
				},
			},
			Data: &commonpb.DataBlob{
				EncodingType: 1,
				Data:         []byte{0xa, 0x23, 0x73, 0x75, 0x62, 0x2d, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x31, 0x2d, 0x73, 0x75, 0x62, 0x2d, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x31, 0x31, 0x2d, 0x64, 0x61, 0x74, 0x61},
			},
		},
		"SubComponent1/SubData11": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          1,
				},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2,
					TransitionCount:          2,
				},
				Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{
					DataAttributes: &persistencespb.ChasmDataAttributes{},
				},
			},
			Data: &commonpb.DataBlob{
				EncodingType: 1,
				Data:         []byte{0xa, 0x19, 0x73, 0x75, 0x62, 0x2d, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x31, 0x2d, 0x73, 0x75, 0x62, 0x2d, 0x64, 0x61, 0x74, 0x61, 0x31, 0x31},
			},
		},
		"SubData1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          1,
				},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2,
					TransitionCount:          2,
				},
				Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{
					DataAttributes: &persistencespb.ChasmDataAttributes{},
				},
			},
			Data: &commonpb.DataBlob{
				EncodingType: 1,
				Data:         []byte{0xa, 0x9, 0x73, 0x75, 0x62, 0x2d, 0x64, 0x61, 0x74, 0x61, 0x31},
			},
		},
	}

	return serializedNodes
}

// Helper functions to regenerate testComponentSerializedNodes() function body.
// Use: generateMapInit(serializedNodes, "serializedNodes")
func generateMapInit(m any, mapName string) {
	val := reflect.ValueOf(m)
	if val.Kind() != reflect.Map {
		fmt.Println("Provided value is not a map")
		return
	}

	keyType := val.Type().Key()
	elemType := val.Type().Elem()

	fmt.Printf("%s := map[%s]%s{\n", mapName, keyType, elemType)

	// Sort string keys for deterministic output
	var keys []reflect.Value
	keys = append(keys, val.MapKeys()...)
	if keyType.Kind() == reflect.String {
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].String() < keys[j].String()
		})
	}

	for _, key := range keys {
		value := val.MapIndex(key)
		fmt.Printf("\t%#v: %s,\n", key.Interface(), renderProtoPointer(value))
	}
	fmt.Println("}")
}

func renderProtoPointer(v reflect.Value) string {
	if v.IsNil() {
		return "nil"
	}

	elem := v.Elem() // Dereference the pointer to struct
	t := elem.Type()
	result := fmt.Sprintf("&%s{\n", t.String())

	for i := 0; i < elem.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported and known proto internal fields
		if field.PkgPath != "" || isProtoInternalField(field.Name) {
			continue
		}

		fieldValue := elem.Field(i)

		// Handle oneof-style interface fields
		if field.Type.Kind() == reflect.Interface && !fieldValue.IsNil() {
			oneofVal := fieldValue.Elem()
			if oneofVal.Kind() == reflect.Ptr {
				oneofVal = oneofVal.Elem()
			}

			result += fmt.Sprintf("\t\t%s: &%s{\n", field.Name, oneofVal.Type().String())
			for j := 0; j < oneofVal.NumField(); j++ {
				oneofField := oneofVal.Type().Field(j)
				if oneofField.PkgPath != "" || isProtoInternalField(oneofField.Name) {
					continue
				}
				oneofFieldValue := oneofVal.Field(j)

				// Handle nested proto inside oneof
				if oneofFieldValue.Kind() == reflect.Ptr && oneofFieldValue.Elem().Kind() == reflect.Struct {
					result += fmt.Sprintf("\t\t\t%s: %s,\n", oneofField.Name, renderProtoPointer(oneofFieldValue))
				} else {
					result += fmt.Sprintf("\t\t\t%s: %#v,\n", oneofField.Name, oneofFieldValue.Interface())
				}
			}
			result += "\t\t},\n"
			continue
		}

		// Recursively handle nested proto messages (pointer to struct)
		if field.Type.Kind() == reflect.Ptr && fieldValue.Kind() == reflect.Ptr && fieldValue.Elem().Kind() == reflect.Struct {
			result += fmt.Sprintf("\t\t%s: %s,\n", field.Name, renderProtoPointer(fieldValue))
			continue
		}

		// Print normal fields
		result += fmt.Sprintf("\t\t%s: %#v,\n", field.Name, fieldValue.Interface())
	}
	result += "\t}"

	result = strings.ReplaceAll(result, "persistence.", "persistencespb.")

	return result
}

func isProtoInternalField(fieldName string) bool {
	switch fieldName {
	case "state", "sizeCache", "unknownFields":
		return true
	default:
		return false
	}
}
