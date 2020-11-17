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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	claimsSystemAdmin = Claims{
		system: RoleAdmin,
	}
	claimsSystemWriter = Claims{
		system: RoleWriter,
	}
	claimsSystemReader = Claims{
		system: RoleReader,
	}
	claimsSystemReaderNamespaceUndefined = Claims{
		system: RoleReader,
		namespaces: map[string]Role{
			"Bar": RoleUndefined,
		},
	}
	claimsSystemUndefinedNamespaceReader = Claims{
		system: RoleUndefined,
		namespaces: map[string]Role{
			"Bar": RoleReader,
		},
	}

	targetFooBar = CallTarget{
		APIName:   "Foo",
		Namespace: "Bar",
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
	result, err := s.authorizer.Authorize(nil, &claimsSystemAdmin, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemWriterAuthZ() {
	result, err := s.authorizer.Authorize(nil, &claimsSystemWriter, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemReaderAuthZ() {
	result, err := s.authorizer.Authorize(nil, &claimsSystemReader, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemReaderBarUndefinedAuthZ() {
	result, err := s.authorizer.Authorize(nil, &claimsSystemReaderNamespaceUndefined, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}
func (s *defaultAuthorizerSuite) TestSystemUndefinedNamespaceReaderAuthZ() {
	result, err := s.authorizer.Authorize(nil, &claimsSystemReaderNamespaceUndefined, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}
