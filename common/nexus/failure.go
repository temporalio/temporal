// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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
	"encoding/json"

	"github.com/nexus-rpc/sdk-go/nexus"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
)

// ProtoFailureToNexusFailure converts a proto Nexus Failure to a Nexus SDK Failure.
// Always returns a non-nil value.
func ProtoFailureToNexusFailure(failure *nexuspb.Failure) *nexus.Failure {
	var details json.RawMessage
	if failure.GetDetails() != nil {
		b, err := json.Marshal(failure.Details)
		// This should never happen, a google.protobuf.Value is always serializable.
		if err != nil {
			panic(err)
		}
		details = json.RawMessage(b)
	}
	return &nexus.Failure{
		Message:  failure.GetMessage(),
		Metadata: failure.GetMetadata(),
		Details:  details,
	}
}

// APIFailureToNexusFailure converts an API proto Failure to a Nexus SDK Failure taking only the failure message to
// avoid leaking too many details to 3rd party callers.
// Always returns a non-nil value.
func APIFailureToNexusFailure(failure *failurepb.Failure) *nexus.Failure {
	return &nexus.Failure{
		Message: failure.GetMessage(),
	}
}
