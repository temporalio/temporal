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

// Code generated by protoc-gen-go-helpers. DO NOT EDIT.
package deployment

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type TaskQueueData to the protobuf v3 wire format
func (val *TaskQueueData) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type TaskQueueData from the protobuf v3 wire format
func (val *TaskQueueData) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *TaskQueueData) Size() int {
	return proto.Size(val)
}

// Equal returns whether two TaskQueueData values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *TaskQueueData) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *TaskQueueData
	switch t := that.(type) {
	case *TaskQueueData:
		that1 = t
	case TaskQueueData:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type DeploymentLocalState to the protobuf v3 wire format
func (val *DeploymentLocalState) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type DeploymentLocalState from the protobuf v3 wire format
func (val *DeploymentLocalState) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *DeploymentLocalState) Size() int {
	return proto.Size(val)
}

// Equal returns whether two DeploymentLocalState values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *DeploymentLocalState) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *DeploymentLocalState
	switch t := that.(type) {
	case *DeploymentLocalState:
		that1 = t
	case DeploymentLocalState:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type DeploymentWorkflowArgs to the protobuf v3 wire format
func (val *DeploymentWorkflowArgs) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type DeploymentWorkflowArgs from the protobuf v3 wire format
func (val *DeploymentWorkflowArgs) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *DeploymentWorkflowArgs) Size() int {
	return proto.Size(val)
}

// Equal returns whether two DeploymentWorkflowArgs values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *DeploymentWorkflowArgs) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *DeploymentWorkflowArgs
	switch t := that.(type) {
	case *DeploymentWorkflowArgs:
		that1 = t
	case DeploymentWorkflowArgs:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type DeploymentSeriesWorkflowArgs to the protobuf v3 wire format
func (val *DeploymentSeriesWorkflowArgs) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type DeploymentSeriesWorkflowArgs from the protobuf v3 wire format
func (val *DeploymentSeriesWorkflowArgs) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *DeploymentSeriesWorkflowArgs) Size() int {
	return proto.Size(val)
}

// Equal returns whether two DeploymentSeriesWorkflowArgs values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *DeploymentSeriesWorkflowArgs) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *DeploymentSeriesWorkflowArgs
	switch t := that.(type) {
	case *DeploymentSeriesWorkflowArgs:
		that1 = t
	case DeploymentSeriesWorkflowArgs:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SeriesLocalState to the protobuf v3 wire format
func (val *SeriesLocalState) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SeriesLocalState from the protobuf v3 wire format
func (val *SeriesLocalState) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SeriesLocalState) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SeriesLocalState values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SeriesLocalState) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SeriesLocalState
	switch t := that.(type) {
	case *SeriesLocalState:
		that1 = t
	case SeriesLocalState:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type RegisterWorkerInDeploymentArgs to the protobuf v3 wire format
func (val *RegisterWorkerInDeploymentArgs) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type RegisterWorkerInDeploymentArgs from the protobuf v3 wire format
func (val *RegisterWorkerInDeploymentArgs) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *RegisterWorkerInDeploymentArgs) Size() int {
	return proto.Size(val)
}

// Equal returns whether two RegisterWorkerInDeploymentArgs values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *RegisterWorkerInDeploymentArgs) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *RegisterWorkerInDeploymentArgs
	switch t := that.(type) {
	case *RegisterWorkerInDeploymentArgs:
		that1 = t
	case RegisterWorkerInDeploymentArgs:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncDeploymentStateArgs to the protobuf v3 wire format
func (val *SyncDeploymentStateArgs) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncDeploymentStateArgs from the protobuf v3 wire format
func (val *SyncDeploymentStateArgs) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncDeploymentStateArgs) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncDeploymentStateArgs values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncDeploymentStateArgs) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncDeploymentStateArgs
	switch t := that.(type) {
	case *SyncDeploymentStateArgs:
		that1 = t
	case SyncDeploymentStateArgs:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncDeploymentStateResponse to the protobuf v3 wire format
func (val *SyncDeploymentStateResponse) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncDeploymentStateResponse from the protobuf v3 wire format
func (val *SyncDeploymentStateResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncDeploymentStateResponse) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncDeploymentStateResponse values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncDeploymentStateResponse) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncDeploymentStateResponse
	switch t := that.(type) {
	case *SyncDeploymentStateResponse:
		that1 = t
	case SyncDeploymentStateResponse:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type QueryDescribeDeploymentResponse to the protobuf v3 wire format
func (val *QueryDescribeDeploymentResponse) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type QueryDescribeDeploymentResponse from the protobuf v3 wire format
func (val *QueryDescribeDeploymentResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *QueryDescribeDeploymentResponse) Size() int {
	return proto.Size(val)
}

// Equal returns whether two QueryDescribeDeploymentResponse values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *QueryDescribeDeploymentResponse) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *QueryDescribeDeploymentResponse
	switch t := that.(type) {
	case *QueryDescribeDeploymentResponse:
		that1 = t
	case QueryDescribeDeploymentResponse:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type DeploymentWorkflowMemo to the protobuf v3 wire format
func (val *DeploymentWorkflowMemo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type DeploymentWorkflowMemo from the protobuf v3 wire format
func (val *DeploymentWorkflowMemo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *DeploymentWorkflowMemo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two DeploymentWorkflowMemo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *DeploymentWorkflowMemo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *DeploymentWorkflowMemo
	switch t := that.(type) {
	case *DeploymentWorkflowMemo:
		that1 = t
	case DeploymentWorkflowMemo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StartDeploymentSeriesRequest to the protobuf v3 wire format
func (val *StartDeploymentSeriesRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StartDeploymentSeriesRequest from the protobuf v3 wire format
func (val *StartDeploymentSeriesRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StartDeploymentSeriesRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StartDeploymentSeriesRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StartDeploymentSeriesRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StartDeploymentSeriesRequest
	switch t := that.(type) {
	case *StartDeploymentSeriesRequest:
		that1 = t
	case StartDeploymentSeriesRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncUserDataRequest to the protobuf v3 wire format
func (val *SyncUserDataRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncUserDataRequest from the protobuf v3 wire format
func (val *SyncUserDataRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncUserDataRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncUserDataRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncUserDataRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncUserDataRequest
	switch t := that.(type) {
	case *SyncUserDataRequest:
		that1 = t
	case SyncUserDataRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncUserDataResponse to the protobuf v3 wire format
func (val *SyncUserDataResponse) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncUserDataResponse from the protobuf v3 wire format
func (val *SyncUserDataResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncUserDataResponse) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncUserDataResponse values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncUserDataResponse) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncUserDataResponse
	switch t := that.(type) {
	case *SyncUserDataResponse:
		that1 = t
	case SyncUserDataResponse:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type CheckUserDataPropagationRequest to the protobuf v3 wire format
func (val *CheckUserDataPropagationRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type CheckUserDataPropagationRequest from the protobuf v3 wire format
func (val *CheckUserDataPropagationRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *CheckUserDataPropagationRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two CheckUserDataPropagationRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *CheckUserDataPropagationRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *CheckUserDataPropagationRequest
	switch t := that.(type) {
	case *CheckUserDataPropagationRequest:
		that1 = t
	case CheckUserDataPropagationRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SetCurrentDeploymentArgs to the protobuf v3 wire format
func (val *SetCurrentDeploymentArgs) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SetCurrentDeploymentArgs from the protobuf v3 wire format
func (val *SetCurrentDeploymentArgs) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SetCurrentDeploymentArgs) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SetCurrentDeploymentArgs values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SetCurrentDeploymentArgs) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SetCurrentDeploymentArgs
	switch t := that.(type) {
	case *SetCurrentDeploymentArgs:
		that1 = t
	case SetCurrentDeploymentArgs:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SetCurrentDeploymentResponse to the protobuf v3 wire format
func (val *SetCurrentDeploymentResponse) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SetCurrentDeploymentResponse from the protobuf v3 wire format
func (val *SetCurrentDeploymentResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SetCurrentDeploymentResponse) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SetCurrentDeploymentResponse values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SetCurrentDeploymentResponse) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SetCurrentDeploymentResponse
	switch t := that.(type) {
	case *SetCurrentDeploymentResponse:
		that1 = t
	case SetCurrentDeploymentResponse:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncDeploymentStateActivityArgs to the protobuf v3 wire format
func (val *SyncDeploymentStateActivityArgs) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncDeploymentStateActivityArgs from the protobuf v3 wire format
func (val *SyncDeploymentStateActivityArgs) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncDeploymentStateActivityArgs) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncDeploymentStateActivityArgs values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncDeploymentStateActivityArgs) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncDeploymentStateActivityArgs
	switch t := that.(type) {
	case *SyncDeploymentStateActivityArgs:
		that1 = t
	case SyncDeploymentStateActivityArgs:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncDeploymentStateActivityResult to the protobuf v3 wire format
func (val *SyncDeploymentStateActivityResult) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncDeploymentStateActivityResult from the protobuf v3 wire format
func (val *SyncDeploymentStateActivityResult) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncDeploymentStateActivityResult) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncDeploymentStateActivityResult values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncDeploymentStateActivityResult) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncDeploymentStateActivityResult
	switch t := that.(type) {
	case *SyncDeploymentStateActivityResult:
		that1 = t
	case SyncDeploymentStateActivityResult:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type DeploymentSeriesWorkflowMemo to the protobuf v3 wire format
func (val *DeploymentSeriesWorkflowMemo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type DeploymentSeriesWorkflowMemo from the protobuf v3 wire format
func (val *DeploymentSeriesWorkflowMemo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *DeploymentSeriesWorkflowMemo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two DeploymentSeriesWorkflowMemo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *DeploymentSeriesWorkflowMemo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *DeploymentSeriesWorkflowMemo
	switch t := that.(type) {
	case *DeploymentSeriesWorkflowMemo:
		that1 = t
	case DeploymentSeriesWorkflowMemo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type IsDeploymentDrainedOfOpenWorkflowsRequest to the protobuf v3 wire format
func (val *IsDeploymentDrainedOfOpenWorkflowsRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type IsDeploymentDrainedOfOpenWorkflowsRequest from the protobuf v3 wire format
func (val *IsDeploymentDrainedOfOpenWorkflowsRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *IsDeploymentDrainedOfOpenWorkflowsRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two IsDeploymentDrainedOfOpenWorkflowsRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *IsDeploymentDrainedOfOpenWorkflowsRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *IsDeploymentDrainedOfOpenWorkflowsRequest
	switch t := that.(type) {
	case *IsDeploymentDrainedOfOpenWorkflowsRequest:
		that1 = t
	case IsDeploymentDrainedOfOpenWorkflowsRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type IsDeploymentDrainedOfOpenWorkflowsResponse to the protobuf v3 wire format
func (val *IsDeploymentDrainedOfOpenWorkflowsResponse) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type IsDeploymentDrainedOfOpenWorkflowsResponse from the protobuf v3 wire format
func (val *IsDeploymentDrainedOfOpenWorkflowsResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *IsDeploymentDrainedOfOpenWorkflowsResponse) Size() int {
	return proto.Size(val)
}

// Equal returns whether two IsDeploymentDrainedOfOpenWorkflowsResponse values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *IsDeploymentDrainedOfOpenWorkflowsResponse) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *IsDeploymentDrainedOfOpenWorkflowsResponse
	switch t := that.(type) {
	case *IsDeploymentDrainedOfOpenWorkflowsResponse:
		that1 = t
	case IsDeploymentDrainedOfOpenWorkflowsResponse:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type IsDeploymentDrainedOfAllWorkflowsRequest to the protobuf v3 wire format
func (val *IsDeploymentDrainedOfAllWorkflowsRequest) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type IsDeploymentDrainedOfAllWorkflowsRequest from the protobuf v3 wire format
func (val *IsDeploymentDrainedOfAllWorkflowsRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *IsDeploymentDrainedOfAllWorkflowsRequest) Size() int {
	return proto.Size(val)
}

// Equal returns whether two IsDeploymentDrainedOfAllWorkflowsRequest values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *IsDeploymentDrainedOfAllWorkflowsRequest) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *IsDeploymentDrainedOfAllWorkflowsRequest
	switch t := that.(type) {
	case *IsDeploymentDrainedOfAllWorkflowsRequest:
		that1 = t
	case IsDeploymentDrainedOfAllWorkflowsRequest:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type IsDeploymentDrainedOfAllWorkflowsResponse to the protobuf v3 wire format
func (val *IsDeploymentDrainedOfAllWorkflowsResponse) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type IsDeploymentDrainedOfAllWorkflowsResponse from the protobuf v3 wire format
func (val *IsDeploymentDrainedOfAllWorkflowsResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *IsDeploymentDrainedOfAllWorkflowsResponse) Size() int {
	return proto.Size(val)
}

// Equal returns whether two IsDeploymentDrainedOfAllWorkflowsResponse values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *IsDeploymentDrainedOfAllWorkflowsResponse) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *IsDeploymentDrainedOfAllWorkflowsResponse
	switch t := that.(type) {
	case *IsDeploymentDrainedOfAllWorkflowsResponse:
		that1 = t
	case IsDeploymentDrainedOfAllWorkflowsResponse:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
