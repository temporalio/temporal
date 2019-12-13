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
	"strings"
	"testing"

	"go.uber.org/yarpc/api/encoding"
	"go.uber.org/yarpc/api/transport"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	VersionCheckerSuite struct {
		*require.Assertions
		suite.Suite
	}
)

func TestVersionCheckerSuite(t *testing.T) {
	suite.Run(t, new(VersionCheckerSuite))
}

func (s *VersionCheckerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *VersionCheckerSuite) TestClientSupported() {
	testCases := []struct {
		callContext              context.Context
		enableClientVersionCheck bool
		expectErr                bool
	}{
		{
			enableClientVersionCheck: false,
			expectErr:                false,
		},
		{
			callContext:              context.Background(),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext("unknown-client", "0.0.0"),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext(GoSDK, "malformed-version"),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext(GoSDK, s.getHigherVersion(SupportedGoSDKVersion)),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext(JavaSDK, s.getHigherVersion(SupportedJavaSDKVersion)),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext(CLI, s.getHigherVersion(SupportedCLIVersion)),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext(GoSDK, SupportedGoSDKVersion),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext(JavaSDK, SupportedJavaSDKVersion),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext(CLI, SupportedCLIVersion),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
	}

	for _, tc := range testCases {
		versionChecker := NewVersionChecker()
		err := versionChecker.ClientSupported(tc.callContext, tc.enableClientVersionCheck)
		if tc.expectErr {
			s.Error(err)
			s.IsType(&shared.ClientVersionNotSupportedError{}, err)
		} else {
			s.NoError(err)
		}
	}
}

func (s *VersionCheckerSuite) TestSupportsStickyQuery() {
	testCases := []struct {
		clientImpl           string
		clientFeatureVersion string
		expectErr            bool
	}{
		{
			clientImpl: "",
			expectErr:  true,
		},
		{
			clientImpl:           "",
			clientFeatureVersion: "0.9.0",
			expectErr:            true,
		},
		{
			clientImpl:           "",
			clientFeatureVersion: "1.0.0",
			expectErr:            false,
		},
		{
			clientImpl: GoSDK,
			expectErr:  true,
		},
		{
			clientImpl:           "unknown",
			clientFeatureVersion: "0.0.0",
			expectErr:            true,
		},
		{
			clientImpl:           GoSDK,
			clientFeatureVersion: "malformed-feature-version",
			expectErr:            true,
		},
		{
			clientImpl:           GoSDK,
			clientFeatureVersion: GoWorkerStickyQueryVersion,
			expectErr:            false,
		},
		{
			clientImpl:           JavaSDK,
			clientFeatureVersion: JavaWorkerStickyQueryVersion,
			expectErr:            false,
		},
		{
			clientImpl:           GoSDK,
			clientFeatureVersion: "0.9.0",
			expectErr:            true,
		},
		{
			clientImpl:           JavaSDK,
			clientFeatureVersion: "0.9.0",
			expectErr:            true,
		},
		{
			clientImpl:           GoSDK,
			clientFeatureVersion: "2.0.0",
			expectErr:            false,
		},
		{
			clientImpl:           JavaSDK,
			clientFeatureVersion: "2.0.0",
			expectErr:            false,
		},
	}

	for _, tc := range testCases {
		vc := NewVersionChecker()
		if tc.expectErr {
			err := vc.SupportsStickyQuery(tc.clientImpl, tc.clientFeatureVersion)
			s.Error(err)
			s.IsType(&shared.ClientVersionNotSupportedError{}, err)
		} else {
			s.NoError(vc.SupportsStickyQuery(tc.clientImpl, tc.clientFeatureVersion))
		}
	}
}

func (s *VersionCheckerSuite) TestSupportsConsistentQuery() {
	testCases := []struct {
		clientImpl           string
		clientFeatureVersion string
		expectErr            bool
	}{
		{
			clientImpl: "",
			expectErr:  true,
		},
		{
			clientImpl:           "",
			clientFeatureVersion: GoWorkerConsistentQueryVersion,
			expectErr:            true,
		},
		{
			clientImpl: GoSDK,
			expectErr:  true,
		},
		{
			clientImpl:           "unknown",
			clientFeatureVersion: "0.0.0",
			expectErr:            true,
		},
		{
			clientImpl:           JavaSDK,
			clientFeatureVersion: "1.5.0",
			expectErr:            true,
		},
		{
			clientImpl:           GoSDK,
			clientFeatureVersion: "malformed-feature-version",
			expectErr:            true,
		},
		{
			clientImpl:           GoSDK,
			clientFeatureVersion: GoWorkerConsistentQueryVersion,
			expectErr:            false,
		},
		{
			clientImpl:           GoSDK,
			clientFeatureVersion: "1.4.0",
			expectErr:            true,
		},
		{
			clientImpl:           GoSDK,
			clientFeatureVersion: "2.0.0",
			expectErr:            false,
		},
	}

	for _, tc := range testCases {
		vc := NewVersionChecker()
		if tc.expectErr {
			err := vc.SupportsConsistentQuery(tc.clientImpl, tc.clientFeatureVersion)
			s.Error(err)
			s.IsType(&shared.ClientVersionNotSupportedError{}, err)
		} else {
			s.NoError(vc.SupportsConsistentQuery(tc.clientImpl, tc.clientFeatureVersion))
		}
	}
}

func (s *VersionCheckerSuite) getHigherVersion(version string) string {
	split := strings.Split(version, ".")
	s.Len(split, 3)
	return fmt.Sprintf("%v.%v.%v", split[0], split[1], split[2][0]-'0'+1)
}

func (s *VersionCheckerSuite) constructCallContext(clientImpl string, featureVersion string) context.Context {
	ctx := context.Background()
	ctx, call := encoding.NewInboundCall(ctx)
	err := call.ReadFromRequest(&transport.Request{
		Headers: transport.NewHeaders().With(common.ClientImplHeaderName, clientImpl).With(common.FeatureVersionHeaderName, featureVersion),
	})
	s.NoError(err)
	return ctx
}
