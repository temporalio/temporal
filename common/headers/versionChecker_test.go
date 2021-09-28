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
	serverVersion := "22.8.78"
	myFeature := "my-new-feature-flag"

	testCases := []struct {
		callContext              context.Context
		enableClientVersionCheck bool
		expectErr                bool
		supportsMyFeature        bool
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
			callContext:              s.constructCallContext("", "unknown-client", "", ""),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext("0.0.0", "", "", ""),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext("0.0.0", "unknown-client", "", ""),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext("malformed-version", ClientNameGoSDK, "", ""),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext("3.0.1", ClientNameGoSDK, "", ""),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext("2.4.5", ClientNameGoSDK, "", ""),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext("2.4.5", ClientNameGoSDK, "<23.1.0", ""),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext("2.4.5", ClientNameGoSDK, ">23.1.0", ""),
			enableClientVersionCheck: true,
			expectErr:                true,
		},
		{
			callContext:              s.constructCallContext("2.4.5", ClientNameGoSDK, "<1.0.0 >=3.5.6 || >22.0.0", ""),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:              s.constructCallContext("", ClientNameGoSDK, "<1.0.0 >=3.5.6 || >22.0.0", ""),
			enableClientVersionCheck: true,
			expectErr:                false,
		},
		{
			callContext:       s.constructCallContext("3.0.5", ClientNameGoSDK, "", myFeature),
			supportsMyFeature: true,
		},
		{
			callContext: s.constructCallContext("3.2.15", ClientNameGoSDK, "",
				strings.Join([]string{"another-feature", myFeature, "third-feature"}, SupportedFeaturesHeaderDelim)),
			supportsMyFeature: true,
		},
	}

	versionChecker := NewVersionChecker(map[string]string{
		ClientNameGoSDK: "<3.0.0",
	}, serverVersion)

	for caseIndex, tc := range testCases {
		err := versionChecker.ClientSupported(tc.callContext, tc.enableClientVersionCheck)
		if tc.expectErr {
			s.Errorf(err, "Case #%d", caseIndex)
			switch err.(type) {
			case *serviceerror.InvalidArgument, *serviceerror.ClientVersionNotSupported, *serviceerror.ServerVersionNotSupported:
			default:
				s.Fail("error has wrong type: %T", err)
			}
		} else {
			s.NoErrorf(err, "Case #%d", caseIndex)
		}

		if tc.callContext != nil {
			s.Equal(tc.supportsMyFeature, versionChecker.ClientSupportsFeature(tc.callContext, myFeature))
		}
	}
}

func (s *VersionCheckerSuite) constructCallContext(clientVersion, clientName, supportedServerVersions, supportedFeatures string) context.Context {
	return SetVersionsForTests(context.Background(), clientVersion, clientName, supportedServerVersions, supportedFeatures)
}
