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

package dynamicconfig

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

// These mock functions are for tests to use config properties that are dynamic

// GetIntPropertyFn returns value as IntPropertyFn
func GetIntPropertyFn(value int) func(opts ...FilterOption) int {
	return func(...FilterOption) int { return value }
}

// GetIntPropertyFilteredByNamespace returns values as IntPropertyFnWithNamespaceFilters
func GetIntPropertyFilteredByNamespace(value int) func(namespace string) int {
	return func(namespace string) int { return value }
}

// GetIntPropertyFilteredByTaskQueueInfo returns value as IntPropertyFnWithTaskQueueInfoFilters
func GetIntPropertyFilteredByTaskQueueInfo(value int) func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) int {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) int { return value }
}

// GetFloatPropertyFn returns value as FloatPropertyFn
func GetFloatPropertyFn(value float64) func(opts ...FilterOption) float64 {
	return func(...FilterOption) float64 { return value }
}

// GetBoolPropertyFn returns value as BoolPropertyFn
func GetBoolPropertyFn(value bool) func(opts ...FilterOption) bool {
	return func(...FilterOption) bool { return value }
}

// GetBoolPropertyFnFilteredByNamespace returns value as BoolPropertyFnWithNamespaceFilters
func GetBoolPropertyFnFilteredByNamespace(value bool) func(namespace string) bool {
	return func(namespace string) bool { return value }
}

// GetDurationPropertyFnFilteredByNamespace returns value as DurationPropertyFnFilteredByNamespace
func GetDurationPropertyFnFilteredByNamespace(value time.Duration) func(namespace string) time.Duration {
	return func(namespace string) time.Duration { return value }
}

// GetDurationPropertyFn returns value as DurationPropertyFn
func GetDurationPropertyFn(value time.Duration) func(opts ...FilterOption) time.Duration {
	return func(...FilterOption) time.Duration { return value }
}

// GetDurationPropertyFnFilteredByTaskQueueInfo returns value as DurationPropertyFnWithTaskQueueInfoFilters
func GetDurationPropertyFnFilteredByTaskQueueInfo(value time.Duration) func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) time.Duration {
	return func(namespace string, taskQueue string, taskType enumspb.TaskQueueType) time.Duration { return value }
}

// GetStringPropertyFn returns value as StringPropertyFn
func GetStringPropertyFn(value string) func(opts ...FilterOption) string {
	return func(...FilterOption) string { return value }
}

// GetMapPropertyFn returns value as MapPropertyFn
func GetMapPropertyFn(value map[string]interface{}) func(opts ...FilterOption) map[string]interface{} {
	return func(...FilterOption) map[string]interface{} { return value }
}

// GetMapPropertyFnWithNamespaceFilter returns value as MapPropertyFn
func GetMapPropertyFnWithNamespaceFilter(value map[string]interface{}) func(namespace string) map[string]interface{} {
	return func(namespace string) map[string]interface{} { return value }
}
