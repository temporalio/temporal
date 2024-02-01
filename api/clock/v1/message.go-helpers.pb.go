// Code generated by protoc-gen-go-helpers. DO NOT EDIT.
package clock

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type VectorClock to the protobuf v3 wire format
func (val *VectorClock) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type VectorClock from the protobuf v3 wire format
func (val *VectorClock) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *VectorClock) Size() int {
	return proto.Size(val)
}

// Equal returns whether two VectorClock values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *VectorClock) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *VectorClock
	switch t := that.(type) {
	case *VectorClock:
		that1 = t
	case VectorClock:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type HybridLogicalClock to the protobuf v3 wire format
func (val *HybridLogicalClock) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HybridLogicalClock from the protobuf v3 wire format
func (val *HybridLogicalClock) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HybridLogicalClock) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HybridLogicalClock values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HybridLogicalClock) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HybridLogicalClock
	switch t := that.(type) {
	case *HybridLogicalClock:
		that1 = t
	case HybridLogicalClock:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
