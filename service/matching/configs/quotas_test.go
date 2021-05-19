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

package configs

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/api/matchingservice/v1"
)

type (
	quotasSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestQuotasSuite(t *testing.T) {
	s := new(quotasSuite)
	suite.Run(t, s)
}

func (s *quotasSuite) SetupSuite() {
}

func (s *quotasSuite) TearDownSuite() {
}

func (s *quotasSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *quotasSuite) TearDownTest() {
}

func (s *quotasSuite) TestAPIToPriorityMapping() {
	mapping := make(map[int]struct{})
	for _, priority := range APIToPriority {
		mapping[priority] = struct{}{}
	}
	s.Equal(mapping, APIPriorities)
}

func (s *quotasSuite) TestAPIs() {
	var service matchingservice.MatchingServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := t.Method(i).Name
		apiToPriority[apiName] = APIToPriority[apiName]
	}
	s.Equal(apiToPriority, APIToPriority)
}
