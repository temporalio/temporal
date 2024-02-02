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
	"google.golang.org/protobuf/types/known/anypb"

	persistencepb "go.temporal.io/server/api/persistence/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
)

func IncomingServiceToEntry(
	service *nexus.IncomingService,
	serviceID string,
	namespaceID string,
	clock *hlc.Clock,
) *persistencepb.NexusIncomingServiceEntry {
	return &persistencepb.NexusIncomingServiceEntry{
		Version: service.Version,
		Id:      serviceID,
		Service: &persistencepb.NexusIncomingService{
			Clock:       clock,
			Name:        service.Name,
			NamespaceId: namespaceID,
			TaskQueue:   service.TaskQueue,
			Metadata:    incomingServiceMetadataToPersisted(service.Metadata, clock),
		},
	}
}

func incomingServiceMetadataToPersisted(
	metadata map[string]*anypb.Any,
	clock *hlc.Clock,
) map[string]*persistencepb.NexusServiceMetadataValue {
	converted := make(map[string]*persistencepb.NexusServiceMetadataValue, len(metadata))
	for k, v := range metadata {
		converted[k] = &persistencepb.NexusServiceMetadataValue{
			Data:                 v,
			State:                persistencepb.NexusServiceMetadataValue_NEXUS_METADATA_STATE_PRESENT,
			StateUpdateTimestamp: clock,
		}
	}
	return converted
}

func IncomingServiceEntryToExternal(
	service *persistencepb.NexusIncomingServiceEntry,
) *nexus.IncomingService {
	return &nexus.IncomingService{
		Version:   service.Version,
		Name:      service.Service.Name,
		Namespace: service.Service.NamespaceId,
		TaskQueue: service.Service.TaskQueue,
		Metadata:  incomingServiceEntryMetadataToExternal(service.Service.Metadata),
	}
}

func incomingServiceEntryMetadataToExternal(
	metadata map[string]*persistencepb.NexusServiceMetadataValue,
) map[string]*anypb.Any {
	converted := make(map[string]*anypb.Any, len(metadata))
	for k, v := range metadata {
		converted[k] = v.Data
	}
	return converted
}
