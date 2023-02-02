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

package frontend

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
)

type (
	redirectionInterceptorSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}
)

func TestRedirectionInterceptorSuite(t *testing.T) {
	s := new(redirectionInterceptorSuite)
	suite.Run(t, s)
}

func (s *redirectionInterceptorSuite) SetupSuite() {
}

func (s *redirectionInterceptorSuite) TearDownSuite() {
}

func (s *redirectionInterceptorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *redirectionInterceptorSuite) TearDownTest() {
}

func (s *redirectionInterceptorSuite) TestAPIMapping() {
	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	expectedAPIs := make(map[string]interface{}, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		expectedAPIs[t.Method(i).Name] = t.Method(i).Type.Out(0)
	}

	actualAPIs := make(map[string]interface{})
	for api, respAllocFn := range localAPIMetrics {
		actualAPIs[api] = reflect.TypeOf(respAllocFn())
	}
	for api, respAllocFn := range globalAPIMetrics {
		actualAPIs[api] = reflect.TypeOf(respAllocFn())
	}
	s.Equal(expectedAPIs, actualAPIs)
}
