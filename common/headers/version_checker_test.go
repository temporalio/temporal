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
		callContext       context.Context
		expectErr         bool
		supportsMyFeature bool
	}{
		{
			callContext: context.Background(),
			expectErr:   false,
		},
		{
			callContext: s.constructCallContext("", "unknown-client", "", ""),
			expectErr:   false,
		},
		{
			callContext: s.constructCallContext("0.0.0", "", "", ""),
			expectErr:   false,
		},
		{
			callContext: s.constructCallContext("0.0.0", "unknown-client", "", ""),
			expectErr:   false,
		},
		{
			callContext: s.constructCallContext("malformed-version", ClientNameGoSDK, "", ""),
			expectErr:   true,
		},
		{
			callContext: s.constructCallContext("3.0.1", ClientNameGoSDK, "", ""),
			expectErr:   true,
		},
		{
			callContext: s.constructCallContext("2.4.5", ClientNameGoSDK, "", ""),
			expectErr:   false,
		},
		{
			callContext: s.constructCallContext("2.4.5", ClientNameGoSDK, "<23.1.0", ""),
			expectErr:   false,
		},
		{
			callContext: s.constructCallContext("2.4.5", ClientNameGoSDK, ">23.1.0", ""),
			expectErr:   true,
		},
		{
			callContext: s.constructCallContext("2.4.5", ClientNameGoSDK, "<1.0.0 >=3.5.6 || >22.0.0", ""),
			expectErr:   false,
		},
		{
			callContext: s.constructCallContext("", ClientNameGoSDK, "<1.0.0 >=3.5.6 || >22.0.0", ""),
			expectErr:   false,
		},
		{
			callContext:       s.constructCallContext("2.4.5", ClientNameGoSDK, "", myFeature),
			supportsMyFeature: true,
		},
		{
			callContext: s.constructCallContext("2.4.5", ClientNameGoSDK, "",
				strings.Join([]string{"another-feature", myFeature, "third-feature"}, SupportedFeaturesHeaderDelim)),
			supportsMyFeature: true,
		},
	}

	versionChecker := NewVersionChecker(map[string]string{
		ClientNameGoSDK: "<3.0.0",
	}, serverVersion)

	for caseIndex, tc := range testCases {
		err := versionChecker.ClientSupported(tc.callContext)
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
