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

package headers

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-version"
	"go.temporal.io/temporal-proto/serviceerror"
	"google.golang.org/grpc/metadata"
)

const (
	// GoSDK is the header value for common.SDKImplHeaderName indicating a Go SDK.
	GoSDK = "temporal-go"
	// JavaSDK is the header value for common.SDKImplHeaderName indicating a Java SDK.
	JavaSDK = "temporal-java"
	// CLI is the header value for common.SDKImplHeaderName indicating a CLI client.
	CLI = "cli"

	// SupportedGoSDKVersion indicates the highest Go SDK feature version server will accept requests from.
	SupportedGoSDKVersion = "0.20.0"
	// SupportedJavaSDKVersion indicates the highest Java SDK feature version server will accept requests from.
	SupportedJavaSDKVersion = "0.20.0"
	// SupportedCLIVersion indicates the highest CLI feature version server will accept requests from.
	SupportedCLIVersion = "0.20.0"

	// GoWorkerConsistentQueryVersion indicates the minimum client version of the Go worker which supports ConsistentQuery.
	// GoWorkerConsistentQueryVersion = "0.20.0"

	// consistentQuery = "consistent-query"
)

type (
	// VersionChecker is used to check client/server compatibility and client's capabilities
	VersionChecker interface {
		ClientSupported(ctx context.Context, enableClientVersionCheck bool) error
		// SupportsConsistentQuery(clientImpl string, clientFeatureVersion string) error
	}

	versionChecker struct {
		supportedFeatures map[string]map[string]version.Constraints
		supportedClients  map[string]version.Constraints
	}
)

var (
	_ VersionChecker = (*versionChecker)(nil)

	versionHeaders = metadata.New(map[string]string{
		SDKVersionHeaderName:        SupportedGoSDKVersion,
		SDKFeatureVersionHeaderName: SupportedGoSDKVersion,
		SDKImplHeaderName:           GoSDK,
	})

	cliVersionHeaders = metadata.New(map[string]string{
		SDKVersionHeaderName:        SupportedCLIVersion,
		SDKFeatureVersionHeaderName: SupportedCLIVersion,
		SDKImplHeaderName:           CLI,
	})
)

// NewVersionChecker constructs a new VersionChecker
func NewVersionChecker() *versionChecker {
	supportedFeatures := map[string]map[string]version.Constraints{
		GoSDK: {
			// consistentQuery: mustNewConstraint(fmt.Sprintf(">=%v", GoWorkerConsistentQueryVersion)),
		},
		JavaSDK: {},
	}
	supportedClients := map[string]version.Constraints{
		GoSDK:   mustNewConstraint(fmt.Sprintf("<=%v", SupportedGoSDKVersion)),
		JavaSDK: mustNewConstraint(fmt.Sprintf("<=%v", SupportedJavaSDKVersion)),
		CLI:     mustNewConstraint(fmt.Sprintf("<=%v", SupportedCLIVersion)),
	}
	return &versionChecker{
		supportedFeatures: supportedFeatures,
		supportedClients:  supportedClients,
	}
}

// ClientSupported returns an error if client is unsupported, nil otherwise.
// In case client version lookup fails assume the client is supported.
func (vc *versionChecker) ClientSupported(ctx context.Context, enableClientVersionCheck bool) error {
	if !enableClientVersionCheck {
		return nil
	}

	headers := GetValues(ctx, SDKFeatureVersionHeaderName, SDKImplHeaderName)
	featureVersion := headers[0]
	clientImpl := headers[1]

	// If client doesn't provide feature version, it means it is fine with any feature set (including current).
	if featureVersion == "" {
		return nil
	}
	// If client doesn't provide client implementation, it means it is fine with any maximum server feature version (including current).
	supportedVersions, ok := vc.supportedClients[clientImpl]
	if !ok {
		return nil
	}
	featureVersionObj, err := version.NewVersion(featureVersion)
	if err != nil {
		return serviceerror.NewClientVersionNotSupported(featureVersion, clientImpl, supportedVersions.String())
	}
	if !supportedVersions.Check(featureVersionObj) {
		return serviceerror.NewClientVersionNotSupported(featureVersion, clientImpl, supportedVersions.String())
	}
	return nil
}

// SupportsConsistentQuery returns error if consistent query is not supported otherwise nil.
// In case client version lookup fails assume the client does not support feature.
// func (vc *versionChecker) SupportsConsistentQuery(clientImpl string, featureVersion string) error {
// 	return vc.featureSupported(clientImpl, featureVersion, consistentQuery)
// }

func (vc *versionChecker) featureSupported(clientImpl string, featureVersion string, feature string) error {
	// If feature version is not provided, it means feature is not supported.
	if featureVersion == "" {
		return serviceerror.NewClientVersionNotSupported(featureVersion, clientImpl, "")
	}
	// If client implementation is not provided, it means feature is not supported.
	implMap, ok := vc.supportedFeatures[clientImpl]
	if !ok {
		return serviceerror.NewClientVersionNotSupported(featureVersion, clientImpl, "")
	}
	supportedVersions, ok := implMap[feature]
	if !ok {
		return serviceerror.NewClientVersionNotSupported(featureVersion, clientImpl, "")
	}
	featureVersionObj, err := version.NewVersion(featureVersion)
	if err != nil {
		return serviceerror.NewClientVersionNotSupported(featureVersion, clientImpl, supportedVersions.String())
	}
	if !supportedVersions.Check(featureVersionObj) {
		return serviceerror.NewClientVersionNotSupported(featureVersion, clientImpl, supportedVersions.String())
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
