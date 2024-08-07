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
	ClusterMemberRole_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Frontend":    1,
		"History":     2,
		"Matching":    3,
		"Worker":      4,
	}
)

// ClusterMemberRoleFromString parses a ClusterMemberRole value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to ClusterMemberRole
func ClusterMemberRoleFromString(s string) (ClusterMemberRole, error) {
	if v, ok := ClusterMemberRole_value[s]; ok {
		return ClusterMemberRole(v), nil
	} else if v, ok := ClusterMemberRole_shorthandValue[s]; ok {
		return ClusterMemberRole(v), nil
	}
	return ClusterMemberRole(0), fmt.Errorf("%s is not a valid ClusterMemberRole", s)
}

var (
	HealthState_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Serving":     1,
		"NotServing":  2,
	}
)

// HealthStateFromString parses a HealthState value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to HealthState
func HealthStateFromString(s string) (HealthState, error) {
	if v, ok := HealthState_value[s]; ok {
		return HealthState(v), nil
	} else if v, ok := HealthState_shorthandValue[s]; ok {
		return HealthState(v), nil
	}
	return HealthState(0), fmt.Errorf("%s is not a valid HealthState", s)
}
