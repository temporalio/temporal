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

package tests

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"go.temporal.io/server/common/primitives"
)

type FunctionalTestBaseSuite struct {
	*require.Assertions
	FunctionalTestBase
	frontendServiceName primitives.ServiceName
	matchingServiceName primitives.ServiceName
	historyServiceName  primitives.ServiceName
	workerServiceName   primitives.ServiceName
}

func (s *FunctionalTestBaseSuite) SetupSuite() {
	s.setupSuite("testdata/es_cluster.yaml",
		WithFxOptionsForService(primitives.FrontendService, fx.Populate(&s.frontendServiceName)),
		WithFxOptionsForService(primitives.MatchingService, fx.Populate(&s.matchingServiceName)),
		WithFxOptionsForService(primitives.HistoryService, fx.Populate(&s.historyServiceName)),
		WithFxOptionsForService(primitives.WorkerService, fx.Populate(&s.workerServiceName)),
	)

}

func (s *FunctionalTestBaseSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *FunctionalTestBaseSuite) TestWithFxOptionsForService() {
	// This test works by using the WithFxOptionsForService option to obtain the ServiceName from the graph, and then
	// it verifies that the ServiceName is correct. It does this because we are targeting the fx.App for a particular
	// service, so we'll know our fx options were provided to the right service if, when we use them to get the current
	// service name, it matches the target service. A more realistic example would use the option to obtain an actual
	// useful object like a history shard controller, or do some graph modifications with fx.Decorate.

	s.Equal(primitives.FrontendService, s.frontendServiceName)
	s.Equal(primitives.MatchingService, s.matchingServiceName)
	s.Equal(primitives.HistoryService, s.historyServiceName)
	s.Equal(primitives.WorkerService, s.workerServiceName)
}

func (s *FunctionalTestBaseSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}
