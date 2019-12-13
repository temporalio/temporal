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
	"go.uber.org/yarpc"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
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

	call := yarpc.CallFromContext(ctx)
	clientFeatureVersion := call.Header(common.FeatureVersionHeaderName)
	clientImpl := call.Header(common.ClientImplHeaderName)

	if clientFeatureVersion == "" {
		return nil
	}
	supportedVersions, ok := vc.supportedClients[clientImpl]
	if !ok {
		return nil
	}
	version, err := version.NewVersion(clientFeatureVersion)
	if err != nil {
		return &shared.ClientVersionNotSupportedError{FeatureVersion: clientFeatureVersion, ClientImpl: clientImpl, SupportedVersions: supportedVersions.String()}
	}
	if !supportedVersions.Check(version) {
		return &shared.ClientVersionNotSupportedError{FeatureVersion: clientFeatureVersion, ClientImpl: clientImpl, SupportedVersions: supportedVersions.String()}
	}
	return nil
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

func (vc *versionChecker) featureSupported(clientImpl string, clientFeatureVersion string, feature string) error {
	// Some older clients may not provide clientImpl.
	// If this is the case special handling needs to be done to maintain backwards compatibility.
	// This can be removed after it is sure there are no existing clients which do not provide clientImpl in RPC headers.
	if clientImpl == "" {
		switch feature {
		case consistentQuery:
			return &shared.ClientVersionNotSupportedError{FeatureVersion: clientFeatureVersion}
		case stickyQuery:
			version, err := version.NewVersion(clientFeatureVersion)
			if err != nil {
				return &shared.ClientVersionNotSupportedError{FeatureVersion: clientFeatureVersion}
			}
			if !vc.stickyQueryUnknownImplConstraints.Check(version) {
				return &shared.ClientVersionNotSupportedError{FeatureVersion: clientFeatureVersion, SupportedVersions: vc.stickyQueryUnknownImplConstraints.String()}
			}
			return nil
		default:
			return ErrUnknownFeature
		}
	}
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
	version, err := version.NewVersion(clientFeatureVersion)
	if err != nil {
		return &shared.ClientVersionNotSupportedError{FeatureVersion: clientFeatureVersion, ClientImpl: clientImpl, SupportedVersions: supportedVersions.String()}
	}
	if !supportedVersions.Check(version) {
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
