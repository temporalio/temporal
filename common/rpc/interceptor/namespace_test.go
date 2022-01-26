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

package interceptor

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

type (
	nameepaceSuite struct {
		suite.Suite
		*require.Assertions
	}
)

var (
	frontendAPIExcluded = map[string]struct{}{
		"GetClusterInfo":      {},
		"GetSystemInfo":       {},
		"GetSearchAttributes": {},
		"ListNamespaces":      {},
	}

	matchingAPIExcluded = map[string]struct{}{
		"ListTaskQueuePartitions": {},
	}

	historyAPIExcluded = map[string]struct{}{
		"CloseShard":                {},
		"GetShard":                  {},
		"GetDLQMessages":            {},
		"GetDLQReplicationMessages": {},
		"GetReplicationMessages":    {},
		"MergeDLQMessages":          {},
		"PurgeDLQMessages":          {},
		"RemoveTask":                {},
		"SyncShardStatus":           {},
		"GetReplicationStatus":      {},
	}
)

func TestNameepaceSuite(t *testing.T) {
	s := new(nameepaceSuite)
	suite.Run(t, s)
}

func (s *nameepaceSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *nameepaceSuite) TearDownTest() {

}

func (s *nameepaceSuite) TestFrontendAPIMetrics() {
	namespaceNameGetter := reflect.TypeOf((*NamespaceNameGetter)(nil)).Elem()

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		methodName := method.Name
		methodType := method.Type

		// 0th parameter is context.Context
		// 1th parameter is the request
		if _, ok := frontendAPIExcluded[methodName]; ok {
			continue
		}
		if methodType.NumIn() < 2 {
			continue
		}
		request := methodType.In(1)
		if !request.Implements(namespaceNameGetter) {
			s.Fail(fmt.Sprintf("API: %v not implementing NamespaceNameGetter", methodName))
		}
	}
}

func (s *nameepaceSuite) TestMatchingAPIMetrics() {
	namespaceIDGetter := reflect.TypeOf((*NamespaceIDGetter)(nil)).Elem()

	var service matchingservice.MatchingServiceServer
	t := reflect.TypeOf(&service).Elem()
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		methodName := method.Name
		methodType := method.Type

		// 0th parameter is context.Context
		// 1th parameter is the request
		if _, ok := matchingAPIExcluded[methodName]; ok {
			continue
		}
		if methodType.NumIn() < 2 {
			continue
		}
		request := methodType.In(1)
		if !request.Implements(namespaceIDGetter) {
			s.Fail(fmt.Sprintf("API: %v not implementing NamespaceIDGetter", methodName))
		}
	}
}

func (s *nameepaceSuite) TestHistoryAPIMetrics() {
	namespaceIDGetter := reflect.TypeOf((*NamespaceIDGetter)(nil)).Elem()

	var service historyservice.HistoryServiceServer
	t := reflect.TypeOf(&service).Elem()
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		methodName := method.Name
		methodType := method.Type

		// 0th parameter is context.Context
		// 1th parameter is the request
		if _, ok := historyAPIExcluded[methodName]; ok {
			continue
		}
		if methodType.NumIn() < 2 {
			continue
		}
		request := methodType.In(1)
		if !request.Implements(namespaceIDGetter) {
			s.Fail(fmt.Sprintf("API: %v not implementing NamespaceIDGetter", methodName))
		}
	}
}
