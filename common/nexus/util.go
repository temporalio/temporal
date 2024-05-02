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

package nexus

import (
	"go.temporal.io/api/nexus/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	persistencepb "go.temporal.io/server/api/persistence/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
)

func EndpointPersistedEntryToExternalAPI(entry *persistencepb.NexusEndpointEntry) *nexus.Endpoint {
	var lastModifiedTime *timestamppb.Timestamp
	// Only set last modified if there were modifications as stated in the UI contract.
	if entry.Version > 1 {
		lastModifiedTime = timestamppb.New(hlc.UTC(entry.Endpoint.Clock))
	}

	return &nexus.Endpoint{
		Version:          entry.Version,
		Id:               entry.Id,
		Spec:             entry.Endpoint.Spec,
		CreatedTime:      entry.Endpoint.CreatedTime,
		LastModifiedTime: lastModifiedTime,
		UrlPrefix:        "/" + RouteDispatchNexusTaskByEndpoint.Path(entry.Id),
	}
}
