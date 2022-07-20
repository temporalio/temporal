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
	claimsSystemAdmin = Claims{
		System: RoleAdmin,
	}
	claimsSystemWriter = Claims{
		System: RoleWriter,
	}
	claimsSystemReader = Claims{
		System: RoleReader,
	}
	claimsSystemReaderNamespaceUndefined = Claims{
		System: RoleReader,
		Namespaces: map[string]Role{
			"bar": RoleUndefined,
		},
	}
	claimsSystemUndefinedNamespaceReader = Claims{
		System: RoleUndefined,
		Namespaces: map[string]Role{
			"bar": RoleReader,
		},
	}

	targetFooBar = CallTarget{
		APIName:   "Foo",
		Namespace: "bar",
	}
	targetFooBAR = CallTarget{
		APIName:   "Foo",
		Namespace: "BAR",
	}
	targetListNamespaces = CallTarget{
		APIName:   "/temporal.api.workflowservice.v1.WorkflowService/ListNamespaces",
		Namespace: "BAR",
	}
	targetDescribeNamespace = CallTarget{
		APIName:   "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
		Namespace: "BAR",
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

func (s *defaultAuthorizerSuite) TestSystemAdminAuthZ() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemAdmin, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemWriterAuthZ() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemWriter, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemReaderAuthZ() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemReader, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemReaderBarUndefinedAuthZ() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemReaderNamespaceUndefined, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemUndefinedNamespaceReaderAuthZ() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemUndefinedNamespaceReader, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemUndefinedNamespaceCaseMismatch() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemUndefinedNamespaceReader, &targetFooBAR)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemUndefinedNamespaceReaderListNamespaces() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemUndefinedNamespaceReader, &targetListNamespaces)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemUndefinedNamespaceReaderDescribeNamespace() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemUndefinedNamespaceReader, &targetDescribeNamespace)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemWriterDescribeNamespace() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemWriter, &targetDescribeNamespace)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemWriterListNamespaces() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemWriter, &targetListNamespaces)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemAdminDescribeNamespace() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemAdmin, &targetDescribeNamespace)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemAdminListNamespaces() {
	result, err := s.authorizer.Authorize(context.TODO(), &claimsSystemAdmin, &targetListNamespaces)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
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
