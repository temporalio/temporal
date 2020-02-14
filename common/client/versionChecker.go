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

package client

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-version"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal/.gen/go/shared"
)

const (
	// GoSDK is the header value for common.ClientImplHeaderName indicating a go sdk client
	GoSDK = "uber-go"
	// JavaSDK is the header value for common.ClientImplHeaderName indicating a java sdk client
	JavaSDK = "uber-java"
	// CLI is the header value for common.ClientImplHeaderName indicating a cli client
	CLI = "cli"

	// SupportedGoSDKVersion indicates the highest go sdk version server will accept requests from
	SupportedGoSDKVersion = "1.5.0"
	// SupportedJavaSDKVersion indicates the highest java sdk version server will accept requests from
	SupportedJavaSDKVersion = "1.5.0"
	// SupportedCLIVersion indicates the highest cli version server will accept requests from
	SupportedCLIVersion = "1.5.0"

	// StickyQueryUnknownImplConstraints indicates the minimum client version of an unknown client type which supports StickyQuery
	StickyQueryUnknownImplConstraints = "1.0.0"
	// GoWorkerStickyQueryVersion indicates the minimum client version of go worker which supports StickyQuery
	GoWorkerStickyQueryVersion = "1.0.0"
	// JavaWorkerStickyQueryVersion indicates the minimum client version of the java worker which supports StickyQuery
	JavaWorkerStickyQueryVersion = "1.0.0"
	// GoWorkerConsistentQueryVersion indicates the minimum client version of the go worker which supports ConsistentQuery
	GoWorkerConsistentQueryVersion = "1.5.0"

	stickyQuery     = "sticky-query"
	consistentQuery = "consistent-query"
)

var (
	// ErrUnknownFeature indicates that requested feature is not known by version checker
	ErrUnknownFeature = &shared.BadRequestError{Message: "Unknown feature"}

	internalHeaders = metadata.New(map[string]string{
		LibraryVersionHeaderName: SupportedGoSDKVersion,
		FeatureVersionHeaderName: GoWorkerConsistentQueryVersion,
		ClientImplHeaderName:     GoSDK,
	})

	cliHeaders = metadata.New(map[string]string{
		LibraryVersionHeaderName: SupportedCLIVersion,
		FeatureVersionHeaderName: GoWorkerConsistentQueryVersion,
		ClientImplHeaderName:     CLI,
	})
)

type (
	// VersionChecker is used to check client/server compatibility and client's capabilities
	VersionChecker interface {
		ClientSupported(ctx context.Context, enableClientVersionCheck bool) error

		SupportsStickyQuery(clientImpl string, clientFeatureVersion string) error
		SupportsConsistentQuery(clientImpl string, clientFeatureVersion string) error
	}

	versionChecker struct {
		supportedFeatures                 map[string]map[string]version.Constraints
		supportedClients                  map[string]version.Constraints
		stickyQueryUnknownImplConstraints version.Constraints
	}
)

// NewVersionChecker constructs a new VersionChecker
func NewVersionChecker() VersionChecker {
	supportedFeatures := map[string]map[string]version.Constraints{
		GoSDK: {
			stickyQuery:     mustNewConstraint(fmt.Sprintf(">=%v", GoWorkerStickyQueryVersion)),
			consistentQuery: mustNewConstraint(fmt.Sprintf(">=%v", GoWorkerConsistentQueryVersion)),
		},
		JavaSDK: {
			stickyQuery: mustNewConstraint(fmt.Sprintf(">=%v", JavaWorkerStickyQueryVersion)),
		},
	}
	supportedClients := map[string]version.Constraints{
		GoSDK:   mustNewConstraint(fmt.Sprintf("<=%v", SupportedGoSDKVersion)),
		JavaSDK: mustNewConstraint(fmt.Sprintf("<=%v", SupportedJavaSDKVersion)),
		CLI:     mustNewConstraint(fmt.Sprintf("<=%v", SupportedCLIVersion)),
	}
	return &versionChecker{
		supportedFeatures:                 supportedFeatures,
		supportedClients:                  supportedClients,
		stickyQueryUnknownImplConstraints: mustNewConstraint(fmt.Sprintf(">=%v", StickyQueryUnknownImplConstraints)),
	}
}

// ClientSupported returns an error if client is unsupported, nil otherwise.
// In case client version lookup fails assume the client is supported.
func (vc *versionChecker) ClientSupported(ctx context.Context, enableClientVersionCheck bool) error {
	if !enableClientVersionCheck {
		return nil
	}

	headers := GetHeadersValue(ctx, FeatureVersionHeaderName, ClientImplHeaderName)
	clientFeatureVersion := headers[0]
	clientImpl := headers[1]

	if clientFeatureVersion == "" {
		return nil
	}
	supportedVersions, ok := vc.supportedClients[clientImpl]
	if !ok {
		return nil
	}
	cfVersion, err := version.NewVersion(clientFeatureVersion)
	if err != nil {
		return &shared.ClientVersionNotSupportedError{FeatureVersion: clientFeatureVersion, ClientImpl: clientImpl, SupportedVersions: supportedVersions.String()}
	}
	if !supportedVersions.Check(cfVersion) {
		return &shared.ClientVersionNotSupportedError{FeatureVersion: clientFeatureVersion, ClientImpl: clientImpl, SupportedVersions: supportedVersions.String()}
	}
	return nil
}

// GetHeadersValue returns header values for passed header names.
// It always returns slice of the same size as number of passed header names.
func GetHeadersValue(ctx context.Context, headerNames ...string) []string {
	headerValues := make([]string, len(headerNames))

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for i, headerName := range headerNames {
			headerValues[i] = getSingleHeaderValue(md, headerName)
		}
	}

	return headerValues
}

// PropagateHeaders propagates headers from incoming context to outgoing context.
func PropagateHeaders(ctx context.Context) context.Context {
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

// SetHeaders sets headers for internal communications.
func SetHeaders(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, internalHeaders)
}

// SetCLIHeaders sets headers for CLI requests.
func SetCLIHeaders(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, cliHeaders)
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

// SetHeaders sets headers as they are received from the client.
// Must be used in tests only.
func SetHeadersForTests(ctx context.Context, libraryVersion, clientImpl, featureVersion string) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		LibraryVersionHeaderName: libraryVersion,
		FeatureVersionHeaderName: featureVersion,
		ClientImplHeaderName:     clientImpl,
	}))
}

// SupportsStickyQuery returns error if sticky query is not supported otherwise nil.
// In case client version lookup fails assume the client does not support feature.
func (vc *versionChecker) SupportsStickyQuery(clientImpl string, clientFeatureVersion string) error {
	return vc.featureSupported(clientImpl, clientFeatureVersion, stickyQuery)
}

// SupportsConsistentQuery returns error if consistent query is not supported otherwise nil.
// In case client version lookup fails assume the client does not support feature.
func (vc *versionChecker) SupportsConsistentQuery(clientImpl string, clientFeatureVersion string) error {
	return vc.featureSupported(clientImpl, clientFeatureVersion, consistentQuery)
}

func getSingleHeaderValue(md metadata.MD, headerName string) string {
	values := md.Get(headerName)
	if len(values) > 0 {
		return values[0]
	} else {
		return ""
	}
}

func (vc *versionChecker) featureSupported(clientImpl string, clientFeatureVersion string, feature string) error {
	if clientFeatureVersion == "" {
		return &shared.ClientVersionNotSupportedError{ClientImpl: clientImpl, FeatureVersion: clientFeatureVersion}
	}
	implMap, ok := vc.supportedFeatures[clientImpl]
	if !ok {
		return &shared.ClientVersionNotSupportedError{ClientImpl: clientImpl, FeatureVersion: clientFeatureVersion}
	}
	supportedVersions, ok := implMap[feature]
	if !ok {
		return &shared.ClientVersionNotSupportedError{ClientImpl: clientImpl, FeatureVersion: clientFeatureVersion}
	}
	cfVersion, err := version.NewVersion(clientFeatureVersion)
	if err != nil {
		return &shared.ClientVersionNotSupportedError{FeatureVersion: clientFeatureVersion, ClientImpl: clientImpl, SupportedVersions: supportedVersions.String()}
	}
	if !supportedVersions.Check(cfVersion) {
		return &shared.ClientVersionNotSupportedError{ClientImpl: clientImpl, FeatureVersion: clientFeatureVersion, SupportedVersions: supportedVersions.String()}
	}
	return nil
}

func mustNewConstraint(v string) version.Constraints {
	constraint, err := version.NewConstraint(v)
	if err != nil {
		panic("invalid version constraint " + v)
	}
	return constraint
}

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
