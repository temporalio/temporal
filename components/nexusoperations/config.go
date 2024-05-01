// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexusoperations

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

// Enabled toggles accepting of API requests and workflow commands that create or modify Nexus operations.
const Enabled = dynamicconfig.Key("component.nexusoperations.enabled")

// RequestTimeout is the timeout for making a single nexus start or cancel request.
const RequestTimeout = dynamicconfig.Key("component.nexusoperations.request.timeout")

// MaxConcurrentOperations limits the maximum allowed concurrent Nexus Operations for a given workflow execution.
// Once the limit is reached, ScheduleNexusOperation commands will be rejected.
const MaxConcurrentOperations = dynamicconfig.Key("component.nexusoperations.limit.operation.concurrency")

// MaxOperationNameLength limits the maximum allowed length for a Nexus Operation name.
// ScheduleNexusOperation commands with an operation name that exceeds this limit will be rejected.
// Uses Go's len() function to determine the length.
const MaxOperationNameLength = dynamicconfig.Key("component.nexusoperations.limit.operation.name.length")

type Config struct {
	Enabled                 dynamicconfig.BoolPropertyFnWithNamespaceFilter
	RequestTimeout          dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxConcurrentOperations dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationNameLength  dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled: dc.GetBoolPropertyFnWithNamespaceFilter(Enabled, false),
		// TODO(bergundy): This should be controllable per namespace + destination.
		RequestTimeout:          dc.GetDurationPropertyFilteredByNamespace(RequestTimeout, time.Second*10),
		MaxConcurrentOperations: dc.GetIntPropertyFilteredByNamespace(MaxConcurrentOperations, 1000),
		MaxOperationNameLength:  dc.GetIntPropertyFilteredByNamespace(MaxOperationNameLength, 1000),
	}
}
