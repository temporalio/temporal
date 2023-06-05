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
	"flag"
	"reflect"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
)

type (
	integrationSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		IntegrationBase
	}
)

func (s *integrationSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.RetentionTimerJitterDuration: time.Second,
		dynamicconfig.EnableEagerWorkflowStart:     true,
	}
	s.setupSuite("testdata/integration_test_cluster.yaml")
}

func (s *integrationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *integrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func TestIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(integrationSuite))
}

func (s *integrationSuite) sendSignal(namespace string, execution *commonpb.WorkflowExecution, signalName string,
	input *commonpb.Payloads, identity string) error {
	_, err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         namespace,
		WorkflowExecution: execution,
		SignalName:        signalName,
		Input:             input,
		Identity:          identity,
	})

	return err
}

func (s *integrationSuite) closeShard(wid string) {
	s.T().Helper()

	resp, err := s.engine.DescribeNamespace(NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: s.namespace,
	})
	s.NoError(err)

	_, err = s.adminClient.CloseShard(NewContext(), &adminservice.CloseShardRequest{
		ShardId: common.WorkflowIDToHistoryShard(resp.NamespaceInfo.Id, wid, s.testClusterConfig.HistoryConfig.NumHistoryShards),
	})
	s.NoError(err)
}

func unmarshalAny[T proto.Message](s *integrationSuite, a *types.Any) T {
	s.T().Helper()
	pb := new(T)
	ppb := reflect.ValueOf(pb).Elem()
	pbNew := reflect.New(reflect.TypeOf(pb).Elem().Elem())
	ppb.Set(pbNew)
	err := types.UnmarshalAny(a, *pb)
	s.NoError(err)
	return *pb
}

func marshalAny(s *integrationSuite, pb proto.Message) *types.Any {
	s.T().Helper()
	a, err := types.MarshalAny(pb)
	s.NoError(err)
	return a
}

func decodeString(s *integrationSuite, pls *commonpb.Payloads) string {
	s.T().Helper()
	var str string
	err := payloads.Decode(pls, &str)
	s.NoError(err)
	return str
}
