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

var Enabled = dynamicconfig.NewNamespaceBoolSetting(
	"component.nexusoperations.enabled",
	false,
	`Enabled toggles accepting of API requests and workflow commands that create or modify Nexus operations.`,
)

var RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
	"component.nexusoperations.request.timeout",
	time.Second*10,
	`RequestTimeout is the timeout for making a single nexus start or cancel request.`,
)

var MaxConcurrentOperations = dynamicconfig.NewNamespaceIntSetting(
	"component.nexusoperations.limit.operation.concurrency",
	1000,
	`MaxConcurrentOperations limits the maximum allowed concurrent Nexus Operations for a given workflow execution.
Once the limit is reached, ScheduleNexusOperation commands will be rejected.`,
)

var MaxServiceNameLength = dynamicconfig.NewNamespaceIntSetting(
	"component.nexusoperations.limit.service.name.length",
	1000,
	`MaxServiceNameLength limits the maximum allowed length for a Nexus Service name.
ScheduleNexusOperation commands with a service name that exceeds this limit will be rejected.
Uses Go's len() function to determine the length.`,
)

var MaxOperationNameLength = dynamicconfig.NewNamespaceIntSetting(
	"component.nexusoperations.limit.operation.name.length",
	1000,
	`MaxOperationNameLength limits the maximum allowed length for a Nexus Operation name.
ScheduleNexusOperation commands with an operation name that exceeds this limit will be rejected.
Uses Go's len() function to determine the length.`,
)

var CallbackURLTemplate = dynamicconfig.NewGlobalStringSetting(
	"component.nexusoperations.callback.endpoint.template",
	"unset",
	`controls the template for generating callback URLs included in Nexus operation requests, which are used to deliver asynchronous completion.
The template can be used to interpolate the {{.NamepaceName}} and {{.NamespaceID}} parameters to construct a publicly accessible URL.
Must be set in order to use Nexus Operations.`,
)

type Config struct {
	Enabled                 dynamicconfig.BoolPropertyFnWithNamespaceFilter
	RequestTimeout          dynamicconfig.DurationPropertyFnWithDestinationFilter
	MaxConcurrentOperations dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxServiceNameLength    dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationNameLength  dynamicconfig.IntPropertyFnWithNamespaceFilter
	CallbackURLTemplate     dynamicconfig.StringPropertyFn
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled:                 Enabled.Get(dc),
		RequestTimeout:          RequestTimeout.Get(dc),
		MaxConcurrentOperations: MaxConcurrentOperations.Get(dc),
		MaxServiceNameLength:    MaxServiceNameLength.Get(dc),
		MaxOperationNameLength:  MaxOperationNameLength.Get(dc),
		CallbackURLTemplate:     CallbackURLTemplate.Get(dc),
	}
}
