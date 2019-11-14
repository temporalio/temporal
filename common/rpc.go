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
	"go.uber.org/yarpc"
	"golang.org/x/net/context"
)

const (
	// LibraryVersionHeaderName refers to the name of the
	// tchannel / http header that contains the client
	// library version
	LibraryVersionHeaderName = "cadence-client-library-version"

	// FeatureVersionHeaderName refers to the name of the
	// tchannel / http header that contains the client
	// feature version
	// the feature version sent from client represents the
	// feature set of the cadence client library support.
	// This can be used for client capibility check, on
	// Cadence server, for backward compatibility
	FeatureVersionHeaderName = "cadence-client-feature-version"

	// ClientImplHeaderName refers to the name of the
	// header that contains the client implementation
	ClientImplHeaderName = "cadence-client-name"
	// EnforceDCRedirection refers to a boolean string of whether
	// to enforce DCRedirection(auto-forwarding)
	// Will be removed in the future: https://github.com/uber/cadence/issues/2304
	EnforceDCRedirection = "cadence-enforce-dc-redirection"
)

type (
	// RPCFactory Creates a dispatcher that knows how to transport requests.
	RPCFactory interface {
		GetDispatcher() *yarpc.Dispatcher
		CreateDispatcherForOutbound(callerName, serviceName, hostName string) *yarpc.Dispatcher
	}
)

// AggregateYarpcOptions aggregate the header information from context to existing yarpc call options
func AggregateYarpcOptions(ctx context.Context, opts ...yarpc.CallOption) []yarpc.CallOption {
	var result []yarpc.CallOption
	if ctx != nil {
		call := yarpc.CallFromContext(ctx)
		for _, key := range call.HeaderNames() {
			value := call.Header(key)
			result = append(result, yarpc.WithHeader(key, value))
		}
	}
	result = append(result, opts...)
	return result
}
