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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
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
			callContext:              s.constructCallContext("0.0.0", "unknown-client", BaseFeaturesFeatureVersion),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext("malformed-version", GoSDK, BaseFeaturesFeatureVersion),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext(s.getHigherVersion(SupportedGoSDKVersion), GoSDK, BaseFeaturesFeatureVersion),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext(s.getHigherVersion(SupportedJavaSDKVersion), JavaSDK, BaseFeaturesFeatureVersion),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext(s.getHigherVersion(SupportedCLIVersion), CLI, BaseFeaturesFeatureVersion),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext(SupportedGoSDKVersion, GoSDK, BaseFeaturesFeatureVersion),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext(SupportedJavaSDKVersion, JavaSDK, BaseFeaturesFeatureVersion),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext(SupportedCLIVersion, CLI, BaseFeaturesFeatureVersion),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
	}

	for caseIndex, tc := range testCases {
		versionChecker := NewVersionChecker()
		err := versionChecker.ClientSupported(tc.callContext, tc.enableClientVersionCheck)
		if tc.expectErr {
			s.Errorf(err, "Case #%d", caseIndex)
			s.IsType(&serviceerror.ClientVersionNotSupported{}, err)
		} else {
			s.NoErrorf(err, "Case #%d", caseIndex)
		}
	}
}

func (s *VersionCheckerSuite) TestSupportsBaseFeatures() {
	testCases := []struct {
		clientFeatureVersion string
		expectErr            bool
	}{
		{
			expectErr: true,
		},
		{
			clientFeatureVersion: BaseFeaturesFeatureVersion,
			expectErr:            false,
		},
		{
			clientFeatureVersion: "0.0.0",
			expectErr:            true,
		},
		{
			clientFeatureVersion: "0.9.0",
			expectErr:            true,
		},
		{
			clientFeatureVersion: "malformed-feature-version",
			expectErr:            true,
		},
		{
			clientFeatureVersion: BaseFeaturesFeatureVersion,
			expectErr:            false,
		},
		{
			clientFeatureVersion: "1.0.0",
			expectErr:            false,
		},
		{
			clientFeatureVersion: "2.0.0",
			expectErr:            false,
		},
	}

	for caseIndex, tc := range testCases {
		vc := NewVersionChecker()
		if tc.expectErr {
			err := vc.SupportsBaseFeatures(tc.clientFeatureVersion)
			s.Errorf(err, "Case #%d", caseIndex)
			s.IsType(&serviceerror.FeatureVersionNotSupported{}, err)
		} else {
			s.NoErrorf(vc.SupportsBaseFeatures(tc.clientFeatureVersion), "Case #%d", caseIndex)
		}
	}
}

func (s *VersionCheckerSuite) getHigherVersion(version string) string {
	split := strings.Split(version, ".")
	s.Len(split, 3)
	return fmt.Sprintf("%v.%v.%v", split[0], split[1], split[2][0]-'0'+1)
}

func (s *VersionCheckerSuite) constructCallContext(clientVersion, clientImpl, featureVersion string) context.Context {
	return SetVersionsForTests(context.Background(), clientVersion, clientImpl, featureVersion)
}
