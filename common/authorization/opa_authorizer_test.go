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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/environment"
)

var ()

type (
	opaAuthorizerSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}

	extendedClaims struct {
		ForceAllow bool
	}
)

func TestOpaAuthorizerSuite(t *testing.T) {
	s := new(opaAuthorizerSuite)
	suite.Run(t, s)
}

func (s *opaAuthorizerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *opaAuthorizerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *opaAuthorizerSuite) TestDeniesWhenOpaDoesNotReturnAllowFalse() {
	opaEndpoint := fmt.Sprintf("%s/v1/data/temporal/deny", getTestOpaBaseUri())
	authorizer := NewOpaAuthorizer(opaEndpoint)
	result, err := authorizer.Authorize(context.TODO(), &claimsSystemAdmin, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}

func (s *opaAuthorizerSuite) TestAllowsWhenOpaReturnsAllowTrue() {
	opaEndpoint := fmt.Sprintf("%s/v1/data/temporal/allow", getTestOpaBaseUri())
	authorizer := NewOpaAuthorizer(opaEndpoint)
	result, err := authorizer.Authorize(context.TODO(), &claimsSystemAdmin, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}

func (s *opaAuthorizerSuite) TestDeniesWhenOpaReturnsNothing() {
	opaEndpoint := fmt.Sprintf("%s/v1/data/temporal/empty", getTestOpaBaseUri())
	authorizer := NewOpaAuthorizer(opaEndpoint)
	result, err := authorizer.Authorize(context.TODO(), &claimsSystemAdmin, &targetFooBar)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}

func (s *opaAuthorizerSuite) TestDeniesWhenOpaPolicyIsNotFound() {
	opaEndpoint := fmt.Sprintf("%s/v1", getTestOpaBaseUri())
	authorizer := NewOpaAuthorizer(opaEndpoint)

	result, err := authorizer.Authorize(context.TODO(), &claimsSystemAdmin, &targetFooBar)
	s.Error(err)
	s.Equal(DecisionDeny, result.Decision)
}

func (s *opaAuthorizerSuite) TestDeniesWhenOpaIsUnreachable() {
	authorizer := NewOpaAuthorizer("http://localhost:8182/v1")
	result, err := authorizer.Authorize(context.TODO(), &claimsSystemAdmin, &targetFooBar)
	s.Error(err)
	s.Equal(DecisionDeny, result.Decision)
}

func (s *opaAuthorizerSuite) TestAllowsWhenOpaHasAccessToClaimExtensions() {
	opaEndpoint := fmt.Sprintf("%s/v1/data/temporal/extensions", getTestOpaBaseUri())
	authorizer := NewOpaAuthorizer(opaEndpoint)

	claims := Claims{
		Extensions: extendedClaims{
			ForceAllow: true,
		},
	}

	target := CallTarget{
		Namespace: "test-namespace",
	}

	result, err := authorizer.Authorize(context.TODO(), &claims, &target)
	s.NoError(err)
	s.Equal(DecisionAllow, result.Decision)
}

func (s *opaAuthorizerSuite) TestDeniesWhenOpaHasAccessToClaimExtensions() {
	opaEndpoint := fmt.Sprintf("%s/v1/data/temporal/extensions", getTestOpaBaseUri())
	authorizer := NewOpaAuthorizer(opaEndpoint)

	claims := Claims{
		Extensions: extendedClaims{
			ForceAllow: false,
		},
	}

	target := CallTarget{}

	result, err := authorizer.Authorize(context.TODO(), &claims, &target)
	s.NoError(err)
	s.Equal(DecisionDeny, result.Decision)
}

func getTestOpaBaseUri() string {
	host := environment.GetOpaHost()
	port := environment.GetOpaPort()

	return fmt.Sprintf("http://%s:%d", host, port)
}
