// Code generated by protoc-gen-go-helpers. DO NOT EDIT.
package token

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type HistoryContinuation to the protobuf v3 wire format
func (val *HistoryContinuation) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HistoryContinuation from the protobuf v3 wire format
func (val *HistoryContinuation) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HistoryContinuation) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HistoryContinuation values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HistoryContinuation) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HistoryContinuation
	switch t := that.(type) {
	case *HistoryContinuation:
		that1 = t
	case HistoryContinuation:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type RawHistoryContinuation to the protobuf v3 wire format
func (val *RawHistoryContinuation) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type RawHistoryContinuation from the protobuf v3 wire format
func (val *RawHistoryContinuation) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *RawHistoryContinuation) Size() int {
	return proto.Size(val)
}

// Equal returns whether two RawHistoryContinuation values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *RawHistoryContinuation) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *RawHistoryContinuation
	switch t := that.(type) {
	case *RawHistoryContinuation:
		that1 = t
	case RawHistoryContinuation:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type Task to the protobuf v3 wire format
func (val *Task) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type Task from the protobuf v3 wire format
func (val *Task) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *Task) Size() int {
	return proto.Size(val)
}

// Equal returns whether two Task values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *Task) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *Task
	switch t := that.(type) {
	case *Task:
		that1 = t
	case Task:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type QueryTask to the protobuf v3 wire format
func (val *QueryTask) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type QueryTask from the protobuf v3 wire format
func (val *QueryTask) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *QueryTask) Size() int {
	return proto.Size(val)
}

// Equal returns whether two QueryTask values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *QueryTask) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *QueryTask
	switch t := that.(type) {
	case *QueryTask:
		that1 = t
	case QueryTask:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NexusTask to the protobuf v3 wire format
func (val *NexusTask) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NexusTask from the protobuf v3 wire format
func (val *NexusTask) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NexusTask) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NexusTask values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NexusTask) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NexusTask
	switch t := that.(type) {
	case *NexusTask:
		that1 = t
	case NexusTask:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
