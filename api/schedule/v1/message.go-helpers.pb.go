// Code generated by protoc-gen-go-helpers. DO NOT EDIT.
package schedule

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type BufferedStart to the protobuf v3 wire format
func (val *BufferedStart) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type BufferedStart from the protobuf v3 wire format
func (val *BufferedStart) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *BufferedStart) Size() int {
	return proto.Size(val)
}

// Equal returns whether two BufferedStart values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *BufferedStart) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *BufferedStart
	switch t := that.(type) {
	case *BufferedStart:
		that1 = t
	case BufferedStart:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type InternalState to the protobuf v3 wire format
func (val *InternalState) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type InternalState from the protobuf v3 wire format
func (val *InternalState) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *InternalState) Size() int {
	return proto.Size(val)
}

// Equal returns whether two InternalState values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *InternalState) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *InternalState
	switch t := that.(type) {
	case *InternalState:
		that1 = t
	case InternalState:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StartScheduleArgs to the protobuf v3 wire format
func (val *StartScheduleArgs) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StartScheduleArgs from the protobuf v3 wire format
func (val *StartScheduleArgs) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StartScheduleArgs) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StartScheduleArgs values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StartScheduleArgs) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StartScheduleArgs
	switch t := that.(type) {
	case *StartScheduleArgs:
		that1 = t
	case StartScheduleArgs:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type FullUpdateRequest to the protobuf v3 wire format
func (val *FullUpdateRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type FullUpdateRequest from the protobuf v3 wire format
func (val *FullUpdateRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *FullUpdateRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two FullUpdateRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *FullUpdateRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *FullUpdateRequest
	switch t := that.(type) {
	case *FullUpdateRequest:
		that1 = t
	case FullUpdateRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type DescribeResponse to the protobuf v3 wire format
func (val *DescribeResponse) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type DescribeResponse from the protobuf v3 wire format
func (val *DescribeResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *DescribeResponse) Size() int {
	return proto.Size(val)
}

// Equal returns whether two DescribeResponse values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *DescribeResponse) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *DescribeResponse
	switch t := that.(type) {
	case *DescribeResponse:
		that1 = t
	case DescribeResponse:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type WatchWorkflowRequest to the protobuf v3 wire format
func (val *WatchWorkflowRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type WatchWorkflowRequest from the protobuf v3 wire format
func (val *WatchWorkflowRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *WatchWorkflowRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two WatchWorkflowRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *WatchWorkflowRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *WatchWorkflowRequest
	switch t := that.(type) {
	case *WatchWorkflowRequest:
		that1 = t
	case WatchWorkflowRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type WatchWorkflowResponse to the protobuf v3 wire format
func (val *WatchWorkflowResponse) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type WatchWorkflowResponse from the protobuf v3 wire format
func (val *WatchWorkflowResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *WatchWorkflowResponse) Size() int {
	return proto.Size(val)
}

// Equal returns whether two WatchWorkflowResponse values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *WatchWorkflowResponse) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *WatchWorkflowResponse
	switch t := that.(type) {
	case *WatchWorkflowResponse:
		that1 = t
	case WatchWorkflowResponse:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StartWorkflowRequest to the protobuf v3 wire format
func (val *StartWorkflowRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StartWorkflowRequest from the protobuf v3 wire format
func (val *StartWorkflowRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StartWorkflowRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StartWorkflowRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StartWorkflowRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StartWorkflowRequest
	switch t := that.(type) {
	case *StartWorkflowRequest:
		that1 = t
	case StartWorkflowRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StartWorkflowResponse to the protobuf v3 wire format
func (val *StartWorkflowResponse) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StartWorkflowResponse from the protobuf v3 wire format
func (val *StartWorkflowResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StartWorkflowResponse) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StartWorkflowResponse values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StartWorkflowResponse) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StartWorkflowResponse
	switch t := that.(type) {
	case *StartWorkflowResponse:
		that1 = t
	case StartWorkflowResponse:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type CancelWorkflowRequest to the protobuf v3 wire format
func (val *CancelWorkflowRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type CancelWorkflowRequest from the protobuf v3 wire format
func (val *CancelWorkflowRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *CancelWorkflowRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two CancelWorkflowRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *CancelWorkflowRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *CancelWorkflowRequest
	switch t := that.(type) {
	case *CancelWorkflowRequest:
		that1 = t
	case CancelWorkflowRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type TerminateWorkflowRequest to the protobuf v3 wire format
func (val *TerminateWorkflowRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type TerminateWorkflowRequest from the protobuf v3 wire format
func (val *TerminateWorkflowRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *TerminateWorkflowRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two TerminateWorkflowRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *TerminateWorkflowRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *TerminateWorkflowRequest
	switch t := that.(type) {
	case *TerminateWorkflowRequest:
		that1 = t
	case TerminateWorkflowRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NextTimeCache to the protobuf v3 wire format
func (val *NextTimeCache) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NextTimeCache from the protobuf v3 wire format
func (val *NextTimeCache) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NextTimeCache) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NextTimeCache values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NextTimeCache) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NextTimeCache
	switch t := that.(type) {
	case *NextTimeCache:
		that1 = t
	case NextTimeCache:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SchedulerInternal to the protobuf v3 wire format
func (val *SchedulerInternal) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SchedulerInternal from the protobuf v3 wire format
func (val *SchedulerInternal) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SchedulerInternal) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SchedulerInternal values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SchedulerInternal) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SchedulerInternal
	switch t := that.(type) {
	case *SchedulerInternal:
		that1 = t
	case SchedulerInternal:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type GeneratorInternal to the protobuf v3 wire format
func (val *GeneratorInternal) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type GeneratorInternal from the protobuf v3 wire format
func (val *GeneratorInternal) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *GeneratorInternal) Size() int {
	return proto.Size(val)
}

// Equal returns whether two GeneratorInternal values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *GeneratorInternal) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *GeneratorInternal
	switch t := that.(type) {
	case *GeneratorInternal:
		that1 = t
	case GeneratorInternal:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type InvokerInternal to the protobuf v3 wire format
func (val *InvokerInternal) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type InvokerInternal from the protobuf v3 wire format
func (val *InvokerInternal) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *InvokerInternal) Size() int {
	return proto.Size(val)
}

// Equal returns whether two InvokerInternal values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *InvokerInternal) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *InvokerInternal
	switch t := that.(type) {
	case *InvokerInternal:
		that1 = t
	case InvokerInternal:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type BackfillerInternal to the protobuf v3 wire format
func (val *BackfillerInternal) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type BackfillerInternal from the protobuf v3 wire format
func (val *BackfillerInternal) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *BackfillerInternal) Size() int {
	return proto.Size(val)
}

// Equal returns whether two BackfillerInternal values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *BackfillerInternal) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *BackfillerInternal
	switch t := that.(type) {
	case *BackfillerInternal:
		that1 = t
	case BackfillerInternal:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
