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
package enums

import (
	"fmt"
)

var (
	DeadLetterQueueType_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Replication": 1,
		"Namespace":   2,
	}
)

// DeadLetterQueueTypeFromString parses a DeadLetterQueueType value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to DeadLetterQueueType
func DeadLetterQueueTypeFromString(s string) (DeadLetterQueueType, error) {
	if v, ok := DeadLetterQueueType_value[s]; ok {
		return DeadLetterQueueType(v), nil
	} else if v, ok := DeadLetterQueueType_shorthandValue[s]; ok {
		return DeadLetterQueueType(v), nil
	}
	return DeadLetterQueueType(0), fmt.Errorf("%s is not a valid DeadLetterQueueType", s)
}

var (
	ChecksumFlavor_shorthandValue = map[string]int32{
		"Unspecified":               0,
		"IeeeCrc32OverProto3Binary": 1,
	}
)

// ChecksumFlavorFromString parses a ChecksumFlavor value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to ChecksumFlavor
func ChecksumFlavorFromString(s string) (ChecksumFlavor, error) {
	if v, ok := ChecksumFlavor_value[s]; ok {
		return ChecksumFlavor(v), nil
	} else if v, ok := ChecksumFlavor_shorthandValue[s]; ok {
		return ChecksumFlavor(v), nil
	}
	return ChecksumFlavor(0), fmt.Errorf("%s is not a valid ChecksumFlavor", s)
}

var (
	SchedulerState_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Waiting":     1,
		"Executing":   2,
	}
)

// SchedulerStateFromString parses a SchedulerState value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to SchedulerState
func SchedulerStateFromString(s string) (SchedulerState, error) {
	if v, ok := SchedulerState_value[s]; ok {
		return SchedulerState(v), nil
	} else if v, ok := SchedulerState_shorthandValue[s]; ok {
		return SchedulerState(v), nil
	}
	return SchedulerState(0), fmt.Errorf("%s is not a valid SchedulerState", s)
}

var (
	CallbackState_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Standby":     1,
		"Scheduled":   2,
		"BackingOff":  3,
		"Failed":      4,
		"Succeeded":   5,
	}
)

// CallbackStateFromString parses a CallbackState value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to CallbackState
func CallbackStateFromString(s string) (CallbackState, error) {
	if v, ok := CallbackState_value[s]; ok {
		return CallbackState(v), nil
	} else if v, ok := CallbackState_shorthandValue[s]; ok {
		return CallbackState(v), nil
	}
	return CallbackState(0), fmt.Errorf("%s is not a valid CallbackState", s)
}
