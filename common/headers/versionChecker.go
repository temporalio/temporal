// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"go.temporal.io/api/serviceerror"
)

const (
	// GoSDK is the header value for common.ClientImplHeaderName indicating a Go SDK.
	GoSDK = "temporal-go"
	// JavaSDK is the header value for common.ClientImplHeaderName indicating a Java SDK.
	JavaSDK = "temporal-java"
	// CLI is the header value for common.ClientImplHeaderName indicating a CLI client.
	CLI = "cli"

	// SupportedGoSDKVersion indicates the highest Go SDK version server will accept requests from.
	SupportedGoSDKVersion = "0.99.0"
	// SupportedJavaSDKVersion indicates the highest Java SDK version server will accept requests from.
	// TODO(maxim): Fix before the first prod release
	SupportedJavaSDKVersion = "0.99.0"
	// SupportedCLIVersion indicates the highest CLI version server will accept requests from.
	SupportedCLIVersion = "0.99.0"

	// BaseFeaturesFeatureVersion indicates the minimum client feature set version which supports all base features.
	BaseFeaturesFeatureVersion = "1.0.0"

	baseFeatures = "base-features"
)

type (
	// VersionChecker is used to check client/server compatibility and client's capabilities
	VersionChecker interface {
		ClientSupported(ctx context.Context, enableClientVersionCheck bool) error
		SupportsBaseFeatures(clientFeatureVersion string) error
	}

	versionChecker struct {
		supportedFeatures map[string]version.Constraints
		supportedClients  map[string]version.Constraints
	}
)

var (
	_ VersionChecker = (*versionChecker)(nil)
)

// NewVersionChecker constructs a new VersionChecker
func NewVersionChecker() *versionChecker {
	// Feature map indicates minimum feature set version for every feature.
	supportedFeatures := map[string]version.Constraints{
		baseFeatures: mustNewConstraint(fmt.Sprintf(">=%v", BaseFeaturesFeatureVersion)),
	}

	// Supported clients map indicates maximum client version that supported by server.
	// See ClientSupported comments for details.
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
//
// This is to prevent NEW clients to connect to the OLD server.
// Sometimes users update SDK to the latest version but forget to update server.
// New SDKs might work incorrectly with old server. This check is to prevent this.
// enableClientVersionCheck param value comes from config and allow to temporary disable client version check.
//
// In case client version lookup fails assume the client is supported.
func (vc *versionChecker) ClientSupported(ctx context.Context, enableClientVersionCheck bool) error {
	if !enableClientVersionCheck {
		return nil
	}

	headers := GetValues(ctx, ClientVersionHeaderName, ClientImplHeaderName)
	clientVersion := headers[0]
	clientImpl := headers[1]

	// If client doesn't provide version, it means it is fine with any server version (including current).
	if clientVersion == "" {
		return nil
	}
	supportedVersion, ok := vc.supportedClients[clientImpl]
	// If client doesn't provide client implementation, it means it is fine with any server version (including current).
	if !ok {
		return nil
	}
	clientVersionObj, err := version.NewVersion(clientVersion)
	if err != nil {
		return serviceerror.NewClientVersionNotSupported(clientVersion, clientImpl, supportedVersion.String())
	}
	if !supportedVersion.Check(clientVersionObj) {
		return serviceerror.NewClientVersionNotSupported(clientVersion, clientImpl, supportedVersion.String())
	}
	return nil
}

// SupportsBaseFeatures returns error if base features is not supported otherwise nil.
// In case client version lookup fails assume the client does not support feature.
func (vc *versionChecker) SupportsBaseFeatures(featureVersion string) error {
	return vc.featureSupported(featureVersion, baseFeatures)
}

func (vc *versionChecker) featureSupported(featureVersion string, feature string) error {
	// If feature version is not provided, it means feature is not supported.
	if featureVersion == "" {
		return serviceerror.NewFeatureVersionNotSupported(feature, featureVersion, "<unknown>")
	}
	supportedFeatureVersion, ok := vc.supportedFeatures[feature]
	if !ok {
		return serviceerror.NewFeatureVersionNotSupported(feature, featureVersion, "<unknown>")
	}
	featureVersionObj, err := version.NewVersion(featureVersion)
	if err != nil {
		return serviceerror.NewFeatureVersionNotSupported(feature, featureVersion, "<unknown>")
	}
	if !supportedFeatureVersion.Check(featureVersionObj) {
		return serviceerror.NewFeatureVersionNotSupported(feature, featureVersion, supportedFeatureVersion.String())
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
