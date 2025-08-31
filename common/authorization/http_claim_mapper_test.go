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
	"crypto/x509/pkix"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	httpClaimMapperSuite struct {
		suite.Suite
		*require.Assertions

		claimMapper ClaimMapper
	}
)

var testAuthInfo = AuthInfo{
	AuthToken: "test-token",
	TLSSubject: &pkix.Name{
		CommonName: "test.example.com",
	},
	ExtraData: "test-extra",
	Audience:  "test-aud",
}

func TestHttpClaimMapperSuite(t *testing.T) {
	s := new(httpClaimMapperSuite)
	suite.Run(t, s)
}
func (s *httpClaimMapperSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.claimMapper = NewHttpClaimMapper()
}

func (s *httpClaimMapperSuite) TestProvidesAuthInfoInExtension() {
	claims, err := s.claimMapper.GetClaims(&testAuthInfo)
	s.Assert().NoError(err)

	extensions, ok := claims.Extensions.(httpClaimExtensions)
	s.Assert().True(ok)

	expectedAuthInfo := httpAuthInfo{
		AuthToken: "test-token",
		TLSSubject: httpAuthTlsInfo{
			CommonName: "test.example.com",
		},
		ExtraData: "test-extra",
		Audience:  "test-aud",
	}

	s.Assert().Equal(extensions.AuthInfo, expectedAuthInfo)
}
