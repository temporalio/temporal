// Copyright (c) 2020 Temporal Technologies, Inc.
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

package headers

import (
	"context"

	"google.golang.org/grpc/metadata"
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
	// EnforceDCRedirectionHeaderName refers to a boolean string of whether
	// to enforce DCRedirection(auto-forwarding)
	// Will be removed in the future: https://github.com/uber/cadence/issues/2304
	EnforceDCRedirectionHeaderName = "cadence-enforce-dc-redirection"
)

// GetValues returns header values for passed header names.
// It always returns slice of the same size as number of passed header names.
func GetValues(ctx context.Context, headerNames ...string) []string {
	headerValues := make([]string, len(headerNames))

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for i, headerName := range headerNames {
			headerValues[i] = getSingleHeaderValue(md, headerName)
		}
	}

	return headerValues
}

// PropagateVersions propagates version headers from incoming context to outgoing context.
// It copies all version headers to outgoing context only if they are exist in incoming context
// and doesn't exist in outgoing context already.
func PropagateVersions(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		outgoingMetadata := copyIncomingHeadersToOutgoing(md,
			LibraryVersionHeaderName,
			FeatureVersionHeaderName,
			ClientImplHeaderName)
		if outgoingMetadata.Len() > 0 {
			ctx = metadata.NewOutgoingContext(ctx, outgoingMetadata)
		}
	}

	return ctx
}

func copyIncomingHeadersToOutgoing(source metadata.MD, headerNames ...string) metadata.MD {
	outgoingMetadata := metadata.New(map[string]string{})
	for _, headerName := range headerNames {
		if values := source.Get(headerName); len(values) > 0 {
			outgoingMetadata.Append(headerName, values...)
		}
	}

	return outgoingMetadata
}

// SetVersions sets headers for internal communications.
func SetVersions(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, versionHeaders)
}

// SetCLIVersions sets headers for CLI requests.
func SetCLIVersions(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, cliVersionHeaders)
}

// SetVersionsForTests sets headers as they would be received from the client.
// Must be used in tests only.
func SetVersionsForTests(ctx context.Context, libraryVersion, clientImpl, featureVersion string) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		LibraryVersionHeaderName: libraryVersion,
		FeatureVersionHeaderName: featureVersion,
		ClientImplHeaderName:     clientImpl,
	}))
}

func getSingleHeaderValue(md metadata.MD, headerName string) string {
	values := md.Get(headerName)
	if len(values) == 0 {
		return ""
	}

	return values[0]
}
