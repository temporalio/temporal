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
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/tqname"
)

type versioningIntegSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	IntegrationBase
	sdkClient sdkclient.Client
}

const (
	partitionTreeDegree = 3
)

func (s *versioningIntegSuite) SetupSuite() {
	s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	s.dynamicConfigOverrides[dynamicconfig.MatchingMaxTaskQueueIdleTime] = 5 * time.Second
	s.dynamicConfigOverrides[dynamicconfig.FrontendEnableWorkerVersioningDataAPIs] = true
	s.dynamicConfigOverrides[dynamicconfig.MatchingForwarderMaxChildrenPerNode] = partitionTreeDegree
	s.setupSuite("testdata/integration_test_cluster.yaml")
}

func (s *versioningIntegSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *versioningIntegSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	clientAddr := "127.0.0.1:7134"
	if TestFlags.FrontendAddr != "" {
		clientAddr = TestFlags.FrontendAddr
	}
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  clientAddr,
		Namespace: s.namespace,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
}

func (s *versioningIntegSuite) TearDownTest() {
	s.sdkClient.Close()
}

func TestVersioningIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(versioningIntegSuite))
}

func (s *versioningIntegSuite) TestBasicVersionUpdate() {
	ctx := NewContext()
	tq := "integration-versioning-basic"

	res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: "foo",
		},
	})
	s.NoError(err)
	s.NotNil(res)

	res2, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal("foo", getCurrentDefault(res2))
}

func (s *versioningIntegSuite) TestSeriesOfUpdates() {
	ctx := NewContext()
	tq := "integration-versioning-series"

	for i := 0; i < 10; i++ {
		res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: s.namespace,
			TaskQueue: tq,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
				AddNewBuildIdInNewDefaultSet: fmt.Sprintf("foo-%d", i),
			},
		})
		s.NoError(err)
		s.NotNil(res)
	}
	res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				NewBuildId:                "foo-2.1",
				ExistingCompatibleBuildId: "foo-2",
				MakeSetDefault:            false,
			},
		},
	})
	s.NoError(err)
	s.NotNil(res)

	res2, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal("foo-9", getCurrentDefault(res2))
	s.Equal("foo-2.1", res2.GetMajorVersionSets()[2].GetBuildIds()[1])
	s.Equal("foo-2", res2.GetMajorVersionSets()[2].GetBuildIds()[0])
}

func (s *versioningIntegSuite) TestLinkToNonexistentCompatibleVersionReturnsNotFound() {
	ctx := NewContext()
	tq := "integration-versioning-compat-not-found"

	res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				NewBuildId:                "foo",
				ExistingCompatibleBuildId: "i don't exist yo",
			},
		},
	})
	s.Error(err)
	s.Nil(res)
	s.IsType(&serviceerror.NotFound{}, err)
}

// This test verifies that user data persists across unload/reload.
func (s *versioningIntegSuite) TestVersioningStateNotDestroyedByOtherUpdates() {
	ctx := NewContext()
	tq := "integration-versioning-not-destroyed"

	res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: "foo",
		},
	})
	s.NoError(err)
	s.NotNil(res)

	// The idle interval has been lowered to 5s in this suite, so we can sleep > 10s to ensure
	// that the task queue is unloaded.
	time.Sleep(11 * time.Second)

	res2, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal("foo", getCurrentDefault(res2))
}

func (s *versioningIntegSuite) TestVersioningChangesPropagate() {
	ctx := NewContext()
	tq := "integration-versioning-propagate"

	// ensure at least two hops
	partCount := 1 + partitionTreeDegree + partitionTreeDegree*partitionTreeDegree

	for _, buildId := range []string{"foo", "foo-v2"} {
		res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: s.namespace,
			TaskQueue: tq,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
				AddNewBuildIdInNewDefaultSet: buildId,
			},
		})
		s.NoError(err)
		s.NotNil(res)

		// Verify partitions have data
		s.Eventually(func() bool {
			for i := 0; i < partCount; i++ {
				partName, err := tqname.FromBaseName(tq)
				s.NoError(err)
				partName = partName.WithPartition(i)
				res, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
					Namespace: s.namespace,
					TaskQueue: partName.FullName(),
				})
				s.NoError(err)
				s.NotNil(res)
				if getCurrentDefault(res) != buildId {
					return false
				}
			}
			return true
		}, 10*time.Second, 100*time.Millisecond)
	}
}

func (s *versioningIntegSuite) testWithMatchingBehavior(subtest func()) {
	dc := s.testCluster.host.dcClient
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)
	defer dc.RemoveOverride(dynamicconfig.TestMatchingLBForceReadPartition)
	defer dc.RemoveOverride(dynamicconfig.TestMatchingLBForceWritePartition)
	defer dc.RemoveOverride(dynamicconfig.TestMatchingDisableSyncMatch)
	for _, forceForward := range []bool{false, true} {
		for _, forceAsync := range []bool{false, true} {
			name := ""
			if forceForward {
				// force two levels of forwarding
				dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 13)
				dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 13)
				dc.OverrideValue(dynamicconfig.TestMatchingLBForceReadPartition, 5)
				dc.OverrideValue(dynamicconfig.TestMatchingLBForceWritePartition, 11)
				name += "ForceForward"
			} else {
				// force single partition
				dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
				dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
				name += "NoForward"
			}
			if forceAsync {
				// disallow sync match to force to db
				dc.OverrideValue(dynamicconfig.TestMatchingDisableSyncMatch, true)
				name += "ForceAsync"
			} else {
				// default value
				dc.OverrideValue(dynamicconfig.TestMatchingDisableSyncMatch, false)
				name += "AllowSync"
			}
			s.Run(name, subtest)
		}
	}
}

func (s *versioningIntegSuite) TestDispatchNewWorkflow() {
	s.testWithMatchingBehavior(s.dispatchNewWorkflow)
}

func (s *versioningIntegSuite) dispatchNewWorkflow() {
	tq := s.randomizeStr(s.T().Name())

	wf := func(ctx workflow.Context) error {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: "v1",
		},
	})
	s.NoError(err)
	time.Sleep(2 * time.Second) // propagation. FIXME: use eventually

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v1",
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))
}

func getCurrentDefault(resp *workflowservice.GetWorkerBuildIdCompatibilityResponse) string {
	if resp == nil {
		return ""
	}
	curMajorSet := resp.GetMajorVersionSets()[len(resp.GetMajorVersionSets())-1]
	return curMajorSet.GetBuildIds()[len(curMajorSet.GetBuildIds())-1]
}
