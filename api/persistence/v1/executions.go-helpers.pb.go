// Code generated by protoc-gen-go-helpers. DO NOT EDIT.
package persistence

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type ShardInfo to the protobuf v3 wire format
func (val *ShardInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ShardInfo from the protobuf v3 wire format
func (val *ShardInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ShardInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ShardInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ShardInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ShardInfo
	switch t := that.(type) {
	case *ShardInfo:
		that1 = t
	case ShardInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type WorkflowExecutionInfo to the protobuf v3 wire format
func (val *WorkflowExecutionInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type WorkflowExecutionInfo from the protobuf v3 wire format
func (val *WorkflowExecutionInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *WorkflowExecutionInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two WorkflowExecutionInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *WorkflowExecutionInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *WorkflowExecutionInfo
	switch t := that.(type) {
	case *WorkflowExecutionInfo:
		that1 = t
	case WorkflowExecutionInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ExecutionStats to the protobuf v3 wire format
func (val *ExecutionStats) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ExecutionStats from the protobuf v3 wire format
func (val *ExecutionStats) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ExecutionStats) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ExecutionStats values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ExecutionStats) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ExecutionStats
	switch t := that.(type) {
	case *ExecutionStats:
		that1 = t
	case ExecutionStats:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type WorkflowExecutionState to the protobuf v3 wire format
func (val *WorkflowExecutionState) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type WorkflowExecutionState from the protobuf v3 wire format
func (val *WorkflowExecutionState) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *WorkflowExecutionState) Size() int {
	return proto.Size(val)
}

// Equal returns whether two WorkflowExecutionState values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *WorkflowExecutionState) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *WorkflowExecutionState
	switch t := that.(type) {
	case *WorkflowExecutionState:
		that1 = t
	case WorkflowExecutionState:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type RequestIDInfo to the protobuf v3 wire format
func (val *RequestIDInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type RequestIDInfo from the protobuf v3 wire format
func (val *RequestIDInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *RequestIDInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two RequestIDInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *RequestIDInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *RequestIDInfo
	switch t := that.(type) {
	case *RequestIDInfo:
		that1 = t
	case RequestIDInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type TransferTaskInfo to the protobuf v3 wire format
func (val *TransferTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type TransferTaskInfo from the protobuf v3 wire format
func (val *TransferTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *TransferTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two TransferTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *TransferTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *TransferTaskInfo
	switch t := that.(type) {
	case *TransferTaskInfo:
		that1 = t
	case TransferTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ReplicationTaskInfo to the protobuf v3 wire format
func (val *ReplicationTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ReplicationTaskInfo from the protobuf v3 wire format
func (val *ReplicationTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ReplicationTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ReplicationTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ReplicationTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ReplicationTaskInfo
	switch t := that.(type) {
	case *ReplicationTaskInfo:
		that1 = t
	case ReplicationTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type VisibilityTaskInfo to the protobuf v3 wire format
func (val *VisibilityTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type VisibilityTaskInfo from the protobuf v3 wire format
func (val *VisibilityTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *VisibilityTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two VisibilityTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *VisibilityTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *VisibilityTaskInfo
	switch t := that.(type) {
	case *VisibilityTaskInfo:
		that1 = t
	case VisibilityTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type TimerTaskInfo to the protobuf v3 wire format
func (val *TimerTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type TimerTaskInfo from the protobuf v3 wire format
func (val *TimerTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *TimerTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two TimerTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *TimerTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *TimerTaskInfo
	switch t := that.(type) {
	case *TimerTaskInfo:
		that1 = t
	case TimerTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ArchivalTaskInfo to the protobuf v3 wire format
func (val *ArchivalTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ArchivalTaskInfo from the protobuf v3 wire format
func (val *ArchivalTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ArchivalTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ArchivalTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ArchivalTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ArchivalTaskInfo
	switch t := that.(type) {
	case *ArchivalTaskInfo:
		that1 = t
	case ArchivalTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type OutboundTaskInfo to the protobuf v3 wire format
func (val *OutboundTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type OutboundTaskInfo from the protobuf v3 wire format
func (val *OutboundTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *OutboundTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two OutboundTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *OutboundTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *OutboundTaskInfo
	switch t := that.(type) {
	case *OutboundTaskInfo:
		that1 = t
	case OutboundTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NexusInvocationTaskInfo to the protobuf v3 wire format
func (val *NexusInvocationTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NexusInvocationTaskInfo from the protobuf v3 wire format
func (val *NexusInvocationTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NexusInvocationTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NexusInvocationTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NexusInvocationTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NexusInvocationTaskInfo
	switch t := that.(type) {
	case *NexusInvocationTaskInfo:
		that1 = t
	case NexusInvocationTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NexusCancelationTaskInfo to the protobuf v3 wire format
func (val *NexusCancelationTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NexusCancelationTaskInfo from the protobuf v3 wire format
func (val *NexusCancelationTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NexusCancelationTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NexusCancelationTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NexusCancelationTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NexusCancelationTaskInfo
	switch t := that.(type) {
	case *NexusCancelationTaskInfo:
		that1 = t
	case NexusCancelationTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ActivityInfo to the protobuf v3 wire format
func (val *ActivityInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ActivityInfo from the protobuf v3 wire format
func (val *ActivityInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ActivityInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ActivityInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ActivityInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ActivityInfo
	switch t := that.(type) {
	case *ActivityInfo:
		that1 = t
	case ActivityInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type TimerInfo to the protobuf v3 wire format
func (val *TimerInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type TimerInfo from the protobuf v3 wire format
func (val *TimerInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *TimerInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two TimerInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *TimerInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *TimerInfo
	switch t := that.(type) {
	case *TimerInfo:
		that1 = t
	case TimerInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ChildExecutionInfo to the protobuf v3 wire format
func (val *ChildExecutionInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ChildExecutionInfo from the protobuf v3 wire format
func (val *ChildExecutionInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ChildExecutionInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ChildExecutionInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ChildExecutionInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ChildExecutionInfo
	switch t := that.(type) {
	case *ChildExecutionInfo:
		that1 = t
	case ChildExecutionInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type RequestCancelInfo to the protobuf v3 wire format
func (val *RequestCancelInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type RequestCancelInfo from the protobuf v3 wire format
func (val *RequestCancelInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *RequestCancelInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two RequestCancelInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *RequestCancelInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *RequestCancelInfo
	switch t := that.(type) {
	case *RequestCancelInfo:
		that1 = t
	case RequestCancelInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SignalInfo to the protobuf v3 wire format
func (val *SignalInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SignalInfo from the protobuf v3 wire format
func (val *SignalInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SignalInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SignalInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SignalInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SignalInfo
	switch t := that.(type) {
	case *SignalInfo:
		that1 = t
	case SignalInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type Checksum to the protobuf v3 wire format
func (val *Checksum) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type Checksum from the protobuf v3 wire format
func (val *Checksum) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *Checksum) Size() int {
	return proto.Size(val)
}

// Equal returns whether two Checksum values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *Checksum) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *Checksum
	switch t := that.(type) {
	case *Checksum:
		that1 = t
	case Checksum:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type Callback to the protobuf v3 wire format
func (val *Callback) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type Callback from the protobuf v3 wire format
func (val *Callback) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *Callback) Size() int {
	return proto.Size(val)
}

// Equal returns whether two Callback values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *Callback) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *Callback
	switch t := that.(type) {
	case *Callback:
		that1 = t
	case Callback:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type HSMCompletionCallbackArg to the protobuf v3 wire format
func (val *HSMCompletionCallbackArg) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HSMCompletionCallbackArg from the protobuf v3 wire format
func (val *HSMCompletionCallbackArg) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HSMCompletionCallbackArg) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HSMCompletionCallbackArg values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HSMCompletionCallbackArg) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HSMCompletionCallbackArg
	switch t := that.(type) {
	case *HSMCompletionCallbackArg:
		that1 = t
	case HSMCompletionCallbackArg:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type CallbackInfo to the protobuf v3 wire format
func (val *CallbackInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type CallbackInfo from the protobuf v3 wire format
func (val *CallbackInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *CallbackInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two CallbackInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *CallbackInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *CallbackInfo
	switch t := that.(type) {
	case *CallbackInfo:
		that1 = t
	case CallbackInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NexusOperationInfo to the protobuf v3 wire format
func (val *NexusOperationInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NexusOperationInfo from the protobuf v3 wire format
func (val *NexusOperationInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NexusOperationInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NexusOperationInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NexusOperationInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NexusOperationInfo
	switch t := that.(type) {
	case *NexusOperationInfo:
		that1 = t
	case NexusOperationInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NexusOperationCancellationInfo to the protobuf v3 wire format
func (val *NexusOperationCancellationInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NexusOperationCancellationInfo from the protobuf v3 wire format
func (val *NexusOperationCancellationInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NexusOperationCancellationInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NexusOperationCancellationInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NexusOperationCancellationInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NexusOperationCancellationInfo
	switch t := that.(type) {
	case *NexusOperationCancellationInfo:
		that1 = t
	case NexusOperationCancellationInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ResetChildInfo to the protobuf v3 wire format
func (val *ResetChildInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ResetChildInfo from the protobuf v3 wire format
func (val *ResetChildInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ResetChildInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ResetChildInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ResetChildInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ResetChildInfo
	switch t := that.(type) {
	case *ResetChildInfo:
		that1 = t
	case ResetChildInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
