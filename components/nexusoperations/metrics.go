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

import "go.temporal.io/server/common/metrics"

var OutboundRequestCounter = metrics.NewCounterDef(
	"nexus_outbound_requests",
	metrics.WithDescription("The number of Nexus outbound requests made by the history service."),
)
var OutboundRequestLatency = metrics.NewTimerDef(
	"nexus_outbound_latency",
	metrics.WithDescription("Latency of outbound Nexus requests made by the history service."),
)

type NexusMetricTagConfig struct {
	// Include service name as a metric tag
	IncludeServiceTag bool
	// Include operation name as a metric tag
	IncludeOperationTag bool
	// Configuration for mapping request headers to metric tags
	HeaderTagMappings []NexusHeaderTagMapping
}

type NexusHeaderTagMapping struct {
	// Name of the request header to extract value from
	SourceHeader string
	// Name of the metric tag to set with the header value
	TargetTag string
}
