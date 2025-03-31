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

// TODO: move this to chasm_test package
package chasm

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	// TestComponent is a sample CHASM component used in tests.
	// It would be nice to move it another package, but this creates a circular dependency.

	protoMessageType = persistencespb.ActivityInfo // Random proto message.
	TestComponent    struct {
		UnimplementedComponent

		ComponentData *protoMessageType
		SubComponent1 Field[*TestSubComponent1]
		SubComponent2 Field[*TestSubComponent2]
		SubData1      Field[*protoMessageType]
	}

	TestSubComponent1 struct {
		UnimplementedComponent

		SubComponent1Data *protoMessageType
		SubComponent11    Field[*TestSubComponent11]
		SubData11         Field[*protoMessageType] // Random proto message.
	}

	TestSubComponent11 struct {
		UnimplementedComponent

		SubComponent11Data *protoMessageType
	}

	TestSubComponent2 struct {
		UnimplementedComponent
		SubComponent2Data protoMessageType
	}
)

func setTestComponentFields(c *TestComponent) {
	c.ComponentData = &protoMessageType{
		ActivityId: "component-data",
	}
	c.SubComponent1 = NewComponentField[*TestSubComponent1](nil, &TestSubComponent1{
		SubComponent1Data: &protoMessageType{
			ActivityId: "sub-component1-data",
		},
		SubComponent11: NewComponentField[*TestSubComponent11](nil, &TestSubComponent11{
			SubComponent11Data: &protoMessageType{ // Random proto type
				ActivityId: "sub-component1-sub-component11-data",
			},
		}),
		SubData11: NewDataField[*protoMessageType](nil, &protoMessageType{
			ActivityId: "sub-component1-sub-data11",
		}),
	})
	c.SubComponent2 = NewEmptyField[*TestSubComponent2]()
	c.SubData1 = NewDataField[*protoMessageType](nil, &protoMessageType{
		ActivityId: "sub-data1",
	})
}

// returns serialized version of TestComponent from above.
func testComponentSerializedNodes() map[string]*persistencespb.ChasmNode {
	serializedNodes := map[string]*persistencespb.ChasmNode{
		"": &persistencespb.ChasmNode{
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
						Type:  "TestLibrary.test_component",
						Tasks: []*persistencespb.ChasmComponentAttributes_Task(nil),
					},
				},
			},
			Data: &commonpb.DataBlob{
				EncodingType: 1,
				Data:         []byte{0x42, 0xe, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x2d, 0x64, 0x61, 0x74, 0x61},
			},
		},
		"SubComponent1": &persistencespb.ChasmNode{
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
						Type:  "TestLibrary.test_sub_component_1",
						Tasks: []*persistencespb.ChasmComponentAttributes_Task(nil),
					},
				},
			},
			Data: &commonpb.DataBlob{
				EncodingType: 1,
				Data:         []byte{0x42, 0x13, 0x73, 0x75, 0x62, 0x2d, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x31, 0x2d, 0x64, 0x61, 0x74, 0x61},
			},
		},
		"SubComponent1/SubComponent11": &persistencespb.ChasmNode{
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
						Type:  "TestLibrary.test_sub_component_11",
						Tasks: []*persistencespb.ChasmComponentAttributes_Task(nil),
					},
				},
			},
			Data: &commonpb.DataBlob{
				EncodingType: 1,
				Data:         []byte{0x42, 0x23, 0x73, 0x75, 0x62, 0x2d, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x31, 0x2d, 0x73, 0x75, 0x62, 0x2d, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x31, 0x31, 0x2d, 0x64, 0x61, 0x74, 0x61},
			},
		},
		"SubComponent1/SubData11": &persistencespb.ChasmNode{
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
				Data:         []byte{0x42, 0x19, 0x73, 0x75, 0x62, 0x2d, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x31, 0x2d, 0x73, 0x75, 0x62, 0x2d, 0x64, 0x61, 0x74, 0x61, 0x31, 0x31},
			},
		},
		"SubData1": &persistencespb.ChasmNode{
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
				Data:         []byte{0x42, 0x9, 0x73, 0x75, 0x62, 0x2d, 0x64, 0x61, 0x74, 0x61, 0x31},
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
	for _, key := range val.MapKeys() {
		keys = append(keys, key)
	}
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
