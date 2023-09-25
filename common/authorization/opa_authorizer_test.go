// The MIT License
//
// Copyright (c) 2023 Manetu Inc.  All rights reserved.
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

package authorization

import (
	"context"
	"go.temporal.io/server/common/log"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
)

type (
	opaAuthorizerSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		authorizer Authorizer
	}
)

func TestOpaAuthorizerSuite(t *testing.T) {
	s := new(opaAuthorizerSuite)
	suite.Run(t, s)
}

func (s *opaAuthorizerSuite) SetupTest() {
	cfg := config.Authorization{Authorizer: "opa"}
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.authorizer = NewOpaAuthorizer(&cfg, log.NewMockLogger(s.controller))
}

func (s *opaAuthorizerSuite) TearDownTest() {
	s.controller.Finish()
}

// largely overlaps with the OPA level unit-tests for default.rego, but proves that the OpaAuthorizer can be a drop-in
// replacement for the DefaultAuthorizer when the internal:default policy is loaded.
func (s *opaAuthorizerSuite) TestAuthorize() {
	testCases := []struct {
		Name     string
		Claims   Claims
		Target   CallTarget
		Decision Decision
	}{
		// SystemAdmin is allowed on everything
		{"SystemAdminOnFooBar", claimsSystemAdmin, targetNamespaceWriteBar, DecisionAllow},
		{"SystemAdminOnAdminAPI", claimsSystemAdmin, targetAdminAPI, DecisionAllow},
		{"SystemAdminOnStartWorkflow", claimsSystemAdmin, targetStartWorkflow, DecisionAllow},

		// SystemWriter is allowed on all read only APIs and non-admin APIs on every namespaces
		{"SystemWriterOnFooBar", claimsSystemWriter, targetNamespaceWriteBar, DecisionAllow},
		{"SystemWriterOnAdminAPI", claimsSystemWriter, targetAdminAPI, DecisionDeny},
		{"SystemWriterOnStartWorkflow", claimsSystemWriter, targetStartWorkflow, DecisionAllow},

		// SystemReader is allowed on all read only APIs and blocked
		{"SystemReaderOnFooBar", claimsSystemReader, targetNamespaceWriteBar, DecisionDeny},
		{"SystemReaderOnAdminAPI", claimsSystemReader, targetAdminAPI, DecisionDeny},
		{"SystemReaderOnStartWorkflow", claimsSystemReader, targetStartWorkflow, DecisionDeny},

		// NamespaceAdmin is allowed on admin service to their own namespaces (test-namespace)
		{"NamespaceAdminOnAdminAPI", claimsNamespaceAdmin, targetAdminAPI, DecisionDeny},
		{"NamespaceAdminOnStartWorkflow", claimsNamespaceAdmin, targetStartWorkflow, DecisionAllow},
		{"NamespaceAdminOnFooBar", claimsNamespaceAdmin, targetNamespaceWriteBar, DecisionDeny}, // namespace mismatch

		{"BarAdminOnFooBar", claimsBarAdmin, targetNamespaceWriteBar, DecisionAllow},
		{"BarAdminOnFooBAR", claimsBarAdmin, targetNamespaceWriteBAR, DecisionDeny}, // namespace case mismatch

		// NamespaceWriter is not allowed on admin APIs
		{"NamespaceWriterOnAdminAPI", claimsNamespaceWriter, targetAdminAPI, DecisionDeny},
		{"NamespaceWriterOnStartWorkflow", claimsNamespaceWriter, targetStartWorkflow, DecisionAllow},
		{"NamespaceWriterOnOperatorNamespaceRead", claimsNamespaceWriter, targetOperatorNamespaceRead, DecisionAllow},
		{"NamespaceWriterOnFooBar", claimsNamespaceWriter, targetNamespaceWriteBar, DecisionDeny}, // namespace mismatch

		// NamespaceReader is allowed on read-only APIs on non admin service
		{"NamespaceReaderOnAdminAPI", claimsNamespaceReader, targetAdminAPI, DecisionDeny},
		{"NamespaceReaderOnStartWorkflow", claimsNamespaceReader, targetStartWorkflow, DecisionDeny},
		{"NamespaceReaderOnFooBar", claimsNamespaceReader, targetNamespaceWriteBar, DecisionDeny}, // namespace mismatch
		{"NamespaceReaderOnListWorkflow", claimsNamespaceReader, targetGetSystemInfo, DecisionAllow},
		{"NamespaceReaderOnOperatorNamespaceRead", claimsNamespaceReader, targetOperatorNamespaceRead, DecisionAllow},

		// healthcheck allowed to everyone
		{"RoleNoneOnGetSystemInfo", claimsNone, targetGetSystemInfo, DecisionAllow},
		{"NamespaceReaderOnGetSystemInfo", claimsNamespaceReader, targetGetSystemInfo, DecisionAllow},
		{"RoleNoneOnHealthCheck", claimsNone, targetGrpcHealthCheck, DecisionAllow},
		{"NamespaceReaderOnHealthCheck", claimsNamespaceReader, targetGrpcHealthCheck, DecisionAllow},
	}

	for _, tt := range testCases {
		result, err := s.authorizer.Authorize(context.TODO(), &tt.Claims, &tt.Target)
		s.NoError(err)
		s.Equal(tt.Decision, result.Decision, "Failed case: %v", tt.Name)
	}
}

func (s *opaAuthorizerSuite) TestGetAuthorizerFromConfigDefault() {
	s.testGetAuthorizerFromConfig("opa", true, reflect.TypeOf(&opaAuthorizer{}))
}
func (s *opaAuthorizerSuite) TestGetAuthorizerFromConfigUnknown() {
	s.testGetAuthorizerFromConfig("foo", false, nil)
}

func (s *opaAuthorizerSuite) testGetAuthorizerFromConfig(name string, valid bool, authorizerType reflect.Type) {

	cfg := config.Authorization{Authorizer: name}
	auth, err := GetAuthorizerFromConfig(&cfg, log.NewMockLogger(s.controller))
	if valid {
		s.NoError(err)
		s.NotNil(auth)
		t := reflect.TypeOf(auth)
		s.True(t == authorizerType)
	} else {
		s.Error(err)
		s.Nil(auth)
	}
}

func (s *opaAuthorizerSuite) TestInternalNoopPolicy() {
	cfg := config.Authorization{Authorizer: "opa", Policies: []string{"internal:noop"}}
	auth, err := GetAuthorizerFromConfig(&cfg, log.NewMockLogger(s.controller))
	s.NoError(err)
	s.NotNil(auth)
}

func (s *opaAuthorizerSuite) TestInvalidInternalPolicy() {
	logger := log.NewMockLogger(s.controller)
	cfg := config.Authorization{Authorizer: "opa", Policies: []string{"internal:invalid"}} // does not exist

	logger.EXPECT().Error("Could not load internal policy", gomock.Any())
	GetAuthorizerFromConfig(&cfg, logger)
}

func (s *opaAuthorizerSuite) TestInlinePolicy() {
	cfg := config.Authorization{Authorizer: "opa", Policies: []string{"package temporal.authz\ndefault allow = true"}}
	auth, err := GetAuthorizerFromConfig(&cfg, log.NewMockLogger(s.controller))
	s.NoError(err)
	s.NotNil(auth)
}

func (s *opaAuthorizerSuite) TestBogusPolicyDefinition() {
	logger := log.NewMockLogger(s.controller)
	cfg := config.Authorization{Authorizer: "opa", Policies: []string{"garbage"}}

	logger.EXPECT().Error("Could not parse policy", gomock.Any())
	GetAuthorizerFromConfig(&cfg, logger)
}
