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

	"github.com/blang/semver/v4"
	"go.temporal.io/api/serviceerror"
)

const (
	ClientNameServer  = "temporal-server"
	ClientNameGoSDK   = "temporal-go"
	ClientNameJavaSDK = "temporal-java"
	ClientNameCLI     = "temporal-cli"

	ServerVersion = "1.1.0"
	CLIVersion    = "1.1.0"

	// SupportedServerVersions is used by CLI and inter role communication.
	SupportedServerVersions = ">=1.0.0 <2.0.0"
)

var (
	SupportedClients = map[string]string{
		ClientNameGoSDK:   "<2.0.0",
		ClientNameJavaSDK: "<2.0.0",
		ClientNameCLI:     "<2.0.0",
		ClientNameServer:  "<2.0.0",
	}
)

type (
	// VersionChecker is used to check client/server compatibility and client's capabilities
	VersionChecker interface {
		ClientSupported(ctx context.Context, enableClientVersionCheck bool) error
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
	vc := &versionChecker{
		serverVersion:         semver.MustParse(serverVersion),
		supportedClients:      supportedClients,
		supportedClientsRange: make(map[string]semver.Range, len(supportedClients)),
	}

	for c, r := range supportedClients {
		vc.supportedClientsRange[c] = semver.MustParseRange(r)
	}

	return vc
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
