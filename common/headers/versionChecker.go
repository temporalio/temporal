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
	"strings"

	"github.com/blang/semver/v4"
	"golang.org/x/exp/slices"

	"go.temporal.io/api/serviceerror"
)

const (
	ClientNameServer        = "temporal-server"
	ClientNameGoSDK         = "temporal-go"
	ClientNameJavaSDK       = "temporal-java"
	ClientNamePHPSDK        = "temporal-php"
	ClientNameTypeScriptSDK = "temporal-typescript"
	ClientNameCLI           = "temporal-cli"
	ClientNameUI            = "temporal-ui"

	ServerVersion = "1.18.0"

	// SupportedServerVersions is used by CLI and inter role communication.
	SupportedServerVersions = ">=1.0.0 <2.0.0"

	// FeatureFollowsNextRunID means that the client supports following next execution run id for
	// completed/failed/timedout completion events when getting the final result of a workflow.
	FeatureFollowsNextRunID = "follows-next-run-id"
)

var (
	// AllFeatures contains all known features. This list is used as the value of the supported
	// features header for internal server requests. There is an assumption that if a feature is
	// defined, then the server itself supports it.
	AllFeatures = strings.Join([]string{
		FeatureFollowsNextRunID,
	}, SupportedFeaturesHeaderDelim)

	SupportedClients = map[string]string{
		ClientNameGoSDK:         "<2.0.0",
		ClientNameJavaSDK:       "<2.0.0",
		ClientNamePHPSDK:        "<2.0.0",
		ClientNameTypeScriptSDK: "<2.0.0",
		ClientNameCLI:           "<2.0.0",
		ClientNameServer:        "<2.0.0",
		ClientNameUI:            "<3.0.0",
	}
)

type (
	// VersionChecker is used to check client/server compatibility and client's capabilities
	VersionChecker interface {
		ClientSupported(ctx context.Context, enableClientVersionCheck bool) error
		ClientSupportsFeature(ctx context.Context, feature string) bool
	}

	versionChecker struct {
		supportedClients      map[string]string
		supportedClientsRange map[string]semver.Range
		serverVersion         semver.Version
	}
)

// NewDefaultVersionChecker constructs a new VersionChecker using default versions from const.
func NewDefaultVersionChecker() *versionChecker {
	return NewVersionChecker(SupportedClients, ServerVersion)
}

// NewVersionChecker constructs a new VersionChecker
func NewVersionChecker(supportedClients map[string]string, serverVersion string) *versionChecker {
	return &versionChecker{
		serverVersion:         semver.MustParse(serverVersion),
		supportedClients:      supportedClients,
		supportedClientsRange: mustParseRanges(supportedClients),
	}
}

// GetClientNameAndVersion extracts SDK name and version from context headers
func GetClientNameAndVersion(ctx context.Context) (string, string) {
	headers := GetValues(ctx, ClientNameHeaderName, ClientVersionHeaderName)
	clientName := headers[0]
	clientVersion := headers[1]
	return clientName, clientVersion
}

// ClientSupported returns an error if client is unsupported, nil otherwise.
func (vc *versionChecker) ClientSupported(ctx context.Context, enableClientVersionCheck bool) error {
	if !enableClientVersionCheck {
		return nil
	}

	headers := GetValues(ctx, ClientNameHeaderName, ClientVersionHeaderName, SupportedServerVersionsHeaderName)
	clientName := headers[0]
	clientVersion := headers[1]
	supportedServerVersions := headers[2]

	// Validate client version only if it is provided and server knows about this client.
	if clientName != "" && clientVersion != "" {
		if supportedClientRange, ok := vc.supportedClientsRange[clientName]; ok {
			clientVersionParsed, parseErr := semver.Parse(clientVersion)
			if parseErr != nil {
				return serviceerror.NewInvalidArgument(fmt.Sprintf("Unable to parse client version: %v", parseErr))
			}
			if !supportedClientRange(clientVersionParsed) {
				return serviceerror.NewClientVersionNotSupported(clientVersion, clientName, vc.supportedClients[clientName])
			}
		}
	}

	// Validate supported server version if it is provided.
	if supportedServerVersions != "" {
		supportedServerVersionsParsed, parseErr := semver.ParseRange(supportedServerVersions)
		if parseErr != nil {
			return serviceerror.NewInvalidArgument(fmt.Sprintf("Unable to parse supported server versions: %v", parseErr))
		}
		if !supportedServerVersionsParsed(vc.serverVersion) {
			return serviceerror.NewServerVersionNotSupported(vc.serverVersion.String(), supportedServerVersions)
		}
	}

	return nil
}

// ClientSupportsFeature returns true if the client reports support for the
// given feature (which should be one of the Feature... constants above).
func (vc *versionChecker) ClientSupportsFeature(ctx context.Context, feature string) bool {
	headers := GetValues(ctx, SupportedFeaturesHeaderName)
	clientFeatures := strings.Split(headers[0], SupportedFeaturesHeaderDelim)
	return slices.Contains(clientFeatures, feature)
}

func mustParseRanges(ranges map[string]string) map[string]semver.Range {
	out := make(map[string]semver.Range, len(ranges))
	for c, r := range ranges {
		out[c] = semver.MustParseRange(r)
	}
	return out
}
