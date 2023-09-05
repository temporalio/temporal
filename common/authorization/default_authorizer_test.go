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

package authorization

import (
	"context"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
)

var (
	claimsNone           = Claims{}
	claimsNamespaceAdmin = Claims{
		Namespaces: map[string]Role{
			testNamespace: RoleAdmin,
		},
	}
	claimsNamespaceWriter = Claims{
		Namespaces: map[string]Role{
			testNamespace: RoleWriter,
		},
	}
	claimsNamespaceReader = Claims{
		Namespaces: map[string]Role{
			testNamespace: RoleReader,
		},
	}
	claimsBarAdmin = Claims{
		Namespaces: map[string]Role{
			"bar": RoleAdmin,
		},
	}
	claimsSystemAdmin = Claims{
		System: RoleAdmin,
	}
	claimsSystemWriter = Claims{
		System: RoleWriter,
	}
	claimsSystemReader = Claims{
		System: RoleReader,
	}
	targetNamespaceWriteBar = CallTarget{
		APIName:   "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted",
		Namespace: "bar",
	}
	targetNamespaceWriteBAR = CallTarget{
		APIName:   "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted",
		Namespace: "BAR",
	}
	targetOperatorNamespaceRead = CallTarget{
		APIName:   "/temporal.api.operatorservice.v1.OperatorService/ListSearchAttributes",
		Namespace: testNamespace,
	}
	targetGrpcHealthCheck = CallTarget{
		APIName:   "/grpc.health.v1.Health/Check",
		Namespace: "",
	}
	targetGetSystemInfo = CallTarget{
		APIName:   "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
		Namespace: "",
	}
	targetStartWorkflow = CallTarget{
		APIName:   "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
		Namespace: testNamespace,
	}
	targetAdminAPI = CallTarget{
		APIName:   "/temporal.server.api.adminservice.v1.AdminService/AddSearchAttributes",
		Namespace: testNamespace,
	}
)

type (
	defaultAuthorizerSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		authorizer Authorizer
	}
)

func TestDefaultAuthorizerSuite(t *testing.T) {
	s := new(defaultAuthorizerSuite)
	suite.Run(t, s)
}

func (s *defaultAuthorizerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.authorizer = NewDefaultAuthorizer()
}

func (s *defaultAuthorizerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *defaultAuthorizerSuite) TestAuthorize() {
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

func (s *defaultAuthorizerSuite) TestGetAuthorizerFromConfigNoop() {
	s.testGetAuthorizerFromConfig("", true, reflect.TypeOf(&noopAuthorizer{}))
}
func (s *defaultAuthorizerSuite) TestGetAuthorizerFromConfigDefault() {
	s.testGetAuthorizerFromConfig("default", true, reflect.TypeOf(&defaultAuthorizer{}))
}
func (s *defaultAuthorizerSuite) TestGetAuthorizerFromConfigUnknown() {
	s.testGetAuthorizerFromConfig("foo", false, nil)
}

func (s *defaultAuthorizerSuite) testGetAuthorizerFromConfig(name string, valid bool, authorizerType reflect.Type) {

	cfg := config.Authorization{Authorizer: name}
	auth, err := GetAuthorizerFromConfig(&cfg)
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
