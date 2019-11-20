// Copyright (c) 2017 Uber Technologies, Inc.
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

package common

import (
	"time"

	"go.uber.org/cadence/.gen/go/shared"

	s "github.com/uber/cadence/.gen/go/shared"
)

// IntPtr makes a copy and returns the pointer to an int.
func IntPtr(v int) *int {
	return &v
}

// Int16Ptr makes a copy and returns the pointer to an int16.
func Int16Ptr(v int16) *int16 {
	return &v
}

// Int32Ptr makes a copy and returns the pointer to an int32.
func Int32Ptr(v int32) *int32 {
	return &v
}

// Int64Ptr makes a copy and returns the pointer to an int64.
func Int64Ptr(v int64) *int64 {
	return &v
}

// Uint32Ptr makes a copy and returns the pointer to a uint32.
func Uint32Ptr(v uint32) *uint32 {
	return &v
}

// Uint64Ptr makes a copy and returns the pointer to a uint64.
func Uint64Ptr(v uint64) *uint64 {
	return &v
}

// Float64Ptr makes a copy and returns the pointer to an int64.
func Float64Ptr(v float64) *float64 {
	return &v
}

// BoolPtr makes a copy and returns the pointer to a bool.
func BoolPtr(v bool) *bool {
	return &v
}

// StringPtr makes a copy and returns the pointer to a string.
func StringPtr(v string) *string {
	return &v
}

// TimeNowNanosPtr returns an int64 ptr to current time in unix nanos
func TimeNowNanosPtr() *int64 {
	v := time.Now().UnixNano()
	return &v
}

// TaskListPtr makes a copy and returns the pointer to a TaskList.
func TaskListPtr(v s.TaskList) *s.TaskList {
	return &v
}

// ActivityTypePtr makes a copy and returns the pointer to a ActivityType.
func ActivityTypePtr(v s.ActivityType) *s.ActivityType {
	return &v
}

// DecisionTypePtr makes a copy and returns the pointer to a DecisionType.
func DecisionTypePtr(t s.DecisionType) *s.DecisionType {
	return &t
}

// ParentClosePolicyPtr makes a copy and returns the pointer to a DecisionType.
func ParentClosePolicyPtr(t s.ParentClosePolicy) *s.ParentClosePolicy {
	return &t
}

// EventTypePtr makes a copy and returns the pointer to a EventType.
func EventTypePtr(t s.EventType) *s.EventType {
	return &t
}

// WorkflowTypePtr makes a copy and returns the pointer to a WorkflowType.
func WorkflowTypePtr(t s.WorkflowType) *s.WorkflowType {
	return &t
}

// TimeoutTypePtr makes a copy and returns the pointer to a TimeoutType.
func TimeoutTypePtr(t s.TimeoutType) *s.TimeoutType {
	return &t
}

// TaskListKindPtr makes a copy and returns the pointer to a TaskListKind.
func TaskListKindPtr(t s.TaskListKind) *s.TaskListKind {
	return &t
}

// TaskListTypePtr makes a copy and returns the pointer to a TaskListKind.
func TaskListTypePtr(t s.TaskListType) *s.TaskListType {
	return &t
}

// DecisionTaskFailedCausePtr makes a copy and returns the pointer to a DecisionTaskFailedCause.
func DecisionTaskFailedCausePtr(t s.DecisionTaskFailedCause) *s.DecisionTaskFailedCause {
	return &t
}

// CancelExternalWorkflowExecutionFailedCausePtr makes a copy and returns the pointer to a CancelExternalWorkflowExecutionFailedCause.
func CancelExternalWorkflowExecutionFailedCausePtr(t s.CancelExternalWorkflowExecutionFailedCause) *s.CancelExternalWorkflowExecutionFailedCause {
	return &t
}

// SignalExternalWorkflowExecutionFailedCausePtr makes a copy and returns the pointer to a SignalExternalWorkflowExecutionFailedCause.
func SignalExternalWorkflowExecutionFailedCausePtr(t s.SignalExternalWorkflowExecutionFailedCause) *s.SignalExternalWorkflowExecutionFailedCause {
	return &t
}

// ChildWorkflowExecutionFailedCausePtr makes a copy and returns the pointer to a ChildWorkflowExecutionFailedCause.
func ChildWorkflowExecutionFailedCausePtr(t s.ChildWorkflowExecutionFailedCause) *s.ChildWorkflowExecutionFailedCause {
	return &t
}

// ArchivalStatusPtr makes a copy and returns the pointer to an ArchivalStatus.
func ArchivalStatusPtr(t s.ArchivalStatus) *s.ArchivalStatus {
	return &t
}

// ClientArchivalStatusPtr makes a copy and returns the pointer to a client ArchivalStatus.
func ClientArchivalStatusPtr(t shared.ArchivalStatus) *shared.ArchivalStatus {
	return &t
}

// QueryResultTypePtr makes a copy and returns the pointer to a QueryResultType
func QueryResultTypePtr(t s.QueryResultType) *s.QueryResultType {
	return &t
}

// QueryRejectConditionPtr makes a copy and returns the pointer to a QueryRejectCondition
func QueryRejectConditionPtr(t s.QueryRejectCondition) *s.QueryRejectCondition {
	return &t
}

// QueryConsistencyLevelPtr makes a copy and returns the pointer to a QueryConsistencyLevel
func QueryConsistencyLevelPtr(t s.QueryConsistencyLevel) *s.QueryConsistencyLevel {
	return &t
}

// QueryTaskCompletedTypePtr makes a copy and returns the pointer to a QueryTaskCompletedType
func QueryTaskCompletedTypePtr(t s.QueryTaskCompletedType) *s.QueryTaskCompletedType {
	return &t
}

// StringDefault returns value if string pointer is set otherwise default value of string
func StringDefault(v *string) string {
	var defaultString string
	if v == nil {
		return defaultString
	}
	return *v
}

// Int32Default returns value if int32 pointer is set otherwise default value of int32
func Int32Default(v *int32) int32 {
	var defaultInt32 int32
	if v == nil {
		return defaultInt32
	}
	return *v
}

// Int64Default returns value if int64 pointer is set otherwise default value of int64
func Int64Default(v *int64) int64 {
	var defaultInt64 int64
	if v == nil {
		return defaultInt64
	}
	return *v
}

// BoolDefault returns value if bool pointer is set otherwise default value of bool
func BoolDefault(v *bool) bool {
	var defaultBool bool
	if v == nil {
		return defaultBool
	}
	return *v
}
