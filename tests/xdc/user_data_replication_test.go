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

//go:build !race

// need to run xdc tests with race detector off because of ringpop bug causing data race issue

package xdc

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/tests"
)

type (
	userDataReplicationTestSuite struct {
		// Embed the base functionality from the XCD test suite
		xdcBaseSuite
	}
)

func TestUserDataReplicationTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(userDataReplicationTestSuite))
}

func (s *userDataReplicationTestSuite) SetupSuite() {
	s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	s.dynamicConfigOverrides[dynamicconfig.EnableWorkerVersioning] = true
	s.setupSuite()
}

func (s *userDataReplicationTestSuite) SetupTest() {
	s.setupTest()
}

func (s *userDataReplicationTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *userDataReplicationTestSuite) TestUserDataIsReplicated() {
	namespace := s.T().Name() + "-" + common.GenerateRandomString(5)
	taskQueue := "versioned"
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(7 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)
	client2 := s.cluster2.GetFrontendClient() // standby

	_, err = client1.UpdateWorkerBuildIdCompatibility(tests.NewContext(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{AddNewBuildIdInNewDefaultSet: "0.1"},
	})
	s.Require().NoError(err)

	s.retry("passive has versioning data", 30, 500*time.Millisecond, func() bool {
		response, err := client2.GetWorkerBuildIdCompatibility(tests.NewContext(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespace,
			TaskQueue: taskQueue,
		})
		s.Require().NoError(err)
		return len(response.GetMajorVersionSets()) == 1
	})
}
