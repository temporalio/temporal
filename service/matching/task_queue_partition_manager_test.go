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

package matching

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/taskqueue/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
)

const (
	namespaceId   = "ns-id"
	namespaceName = "ns-name"
	taskQueueName = "my-test-tq"
)

type PartitionManagerTestSuite struct {
	suite.Suite
	controller   *gomock.Controller
	userDataMgr  *mockUserDataManager
	partitionMgr *taskQueuePartitionManagerImpl
}

func TestPartitionManagerSuite(t *testing.T) {
	suite.Run(t, new(PartitionManagerTestSuite))
}

func (s *PartitionManagerTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	ns, registry := createMockNamespaceCache(s.controller, namespace.Name(namespaceName))
	config := NewConfig(dynamicconfig.NewNoopCollection())
	matchingClientMock := matchingservicemock.NewMockMatchingServiceClient(s.controller)
	me := createTestMatchingEngine(s.controller, config, matchingClientMock, registry)
	f, err := tqid.NewTaskQueueFamily(namespaceId, taskQueueName)
	s.Assert().NoError(err)
	partition := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition()
	tqConfig := newTaskQueueConfig(partition.TaskQueue(), me.config, ns.Name())
	s.userDataMgr = &mockUserDataManager{}
	pm, err := newTaskQueuePartitionManager(me, ns, partition, tqConfig, s.userDataMgr)
	s.Assert().NoError(err)
	s.partitionMgr = pm
	me.Start()
	pm.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = pm.WaitUntilInitialized(ctx)
	s.Assert().NoError(err)
}

func (s *PartitionManagerTestSuite) TestAddTask_Forwarded() {
	_, _, err := s.partitionMgr.AddTask(context.Background(), addTaskParams{
		taskInfo: &persistence.TaskInfo{
			NamespaceId: namespaceId,
			RunId:       "run",
			WorkflowId:  "wf",
		},
		forwardInfo: &taskqueue.TaskForwardInfo{SourcePartition: "another-partition"},
	})
	s.Assert().Equal(errRemoteSyncMatchFailed, err)
}

func (s *PartitionManagerTestSuite) TestAddTaskNoRules_NoVersionDirective() {
	s.validateAddTask("", false, nil, nil)
	s.validatePollTask("", false)

	// a poller with non-empty build ID should go to unversioned queue when useVersioning=false
	s.validateAddTask("", false, nil, nil)
	s.validatePollTask("buildXYZ", false)
}

func (s *PartitionManagerTestSuite) TestAddTaskNoRules_AssignedTask() {
	bld := "buildXYZ"
	s.validateAddTask("", false, nil, worker_versioning.MakeBuildIdDirective(bld))
	s.validatePollTask(bld, true)
}

func (s *PartitionManagerTestSuite) TestAddTaskNoRules_UnassignedTask() {
	s.validateAddTask("", false, nil, worker_versioning.MakeUseAssignmentRulesDirective())
	s.validatePollTask("", false)
}

func (s *PartitionManagerTestSuite) TestPollWithRedirectRules() {
	source := "bld1"
	target := "bld2"
	versioningData := &persistence.VersioningData{
		RedirectRules: []*persistence.RedirectRule{
			{
				Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{
					SourceBuildId: source,
					TargetBuildId: target,
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(source))

	s.validatePollTask(target, true)

	_, _, err := s.partitionMgr.PollTask(ctx, &pollMetadata{
		workerVersionCapabilities: &common.WorkerVersionCapabilities{
			BuildId:       source,
			UseVersioning: true,
		},
	})
	s.Assert().Equal(serviceerror.NewNewerBuildExists(target), err)
}

func (s *PartitionManagerTestSuite) TestRedirectRuleLoadUpstream() {
	source := "bld1"
	target := "bld2"
	versioningData := &persistence.VersioningData{
		RedirectRules: []*persistence.RedirectRule{
			{
				Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{
					SourceBuildId: source,
					TargetBuildId: target,
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(source))

	// task is backlogged in the source queue so it is loaded by now
	sourceQ, err := s.partitionMgr.getVersionedQueue(ctx, "", source, false)
	s.Assert().NoError(err)
	s.Assert().NotNil(sourceQ)

	// unload sourceQ
	s.partitionMgr.unloadPhysicalQueue(sourceQ)

	// poll from target
	s.validatePollTask(target, true)

	// polling from target should've loaded the source as well
	sourceQ, err = s.partitionMgr.getVersionedQueue(ctx, "", source, false)
	s.Assert().NoError(err)
	s.Assert().NotNil(sourceQ)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_NoVersionDirective() {
	buildId := "bld"
	versioningData := &persistence.VersioningData{AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(buildId)}}
	s.validateAddTask("", false, versioningData, nil)
	s.validatePollTask("", false)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_AssignedTask() {
	ruleBld := "rule-bld"
	versioningData := &persistence.VersioningData{AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)}}
	taskBld := "task-bld"
	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(taskBld))
	s.validatePollTask(taskBld, true)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_UnassignedTask() {
	ruleBld := "rule-bld"
	versioningData := &persistence.VersioningData{AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)}}
	s.validateAddTask(ruleBld, false, versioningData, worker_versioning.MakeUseAssignmentRulesDirective())
	s.validatePollTask(ruleBld, true)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_UnassignedTask_SyncMatch() {
	ruleBld := "rule-bld"
	versioningData := &persistence.VersioningData{AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)}}
	s.validatePollTaskSyncMatch(ruleBld, true)
	s.validateAddTask("", true, versioningData, worker_versioning.MakeUseAssignmentRulesDirective())
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRulesAndVersionSets_NoVersionDirective() {
	ruleBld := "rule-bld"
	vs := createVersionSet("vs-bld")
	versioningData := &persistence.VersioningData{
		AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)},
		VersionSets:     []*persistence.CompatibleVersionSet{vs},
	}

	s.validateAddTask("", false, versioningData, nil)
	// make sure version set queue is not loaded
	s.Assert().Nil(s.partitionMgr.versionedQueues[vs.SetIds[0]])
	s.validatePollTask("", false)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRulesAndVersionSets_AssignedTask() {
	ruleBld := "rule-bld"
	vs := createVersionSet("vs-bld")
	versioningData := &persistence.VersioningData{
		AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)},
		VersionSets:     []*persistence.CompatibleVersionSet{vs},
	}

	taskBld := "task-bld"
	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(taskBld))
	// make sure version set queue is not loaded
	s.Assert().Nil(s.partitionMgr.versionedQueues[vs.SetIds[0]])
	s.validatePollTask(taskBld, true)

	// now use the version set build ID
	s.validateAddTask("", false, versioningData, worker_versioning.MakeBuildIdDirective(vs.BuildIds[0].Id))
	// make sure version set queue is loaded
	s.Assert().NotNil(s.partitionMgr.versionedQueues[vs.SetIds[0]])
	s.validatePollTask(vs.BuildIds[0].Id, true)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRulesAndVersionSets_UnassignedTask() {
	ruleBld := "rule-bld"
	vs := createVersionSet("vs-bld")
	versioningData := &persistence.VersioningData{
		AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)},
		VersionSets:     []*persistence.CompatibleVersionSet{vs},
	}
	s.validateAddTask(ruleBld, false, versioningData, worker_versioning.MakeUseAssignmentRulesDirective())
	// make sure version set queue is not loaded
	s.Assert().Nil(s.partitionMgr.versionedQueues[vs.SetIds[0]])
	s.validatePollTask(ruleBld, true)
}

func (s *PartitionManagerTestSuite) TestGetAllPollerInfo() {
	// no pollers
	pollers := s.partitionMgr.GetAllPollerInfo()
	s.Assert().True(len(pollers) == 0)

	// one unversioned poller
	s.pollWithIdentity("uv", "", false)
	pollers = s.partitionMgr.GetAllPollerInfo()
	s.Assert().True(len(pollers) == 1)

	// one versioned poller
	s.pollWithIdentity("v", "bid", true)
	pollers = s.partitionMgr.GetAllPollerInfo()
	s.Assert().True(len(pollers) == 2)

	for _, p := range pollers {
		switch p.GetIdentity() {
		case "uv":
			s.Assert().False(p.GetWorkerVersionCapabilities().GetUseVersioning())
		case "v":
			s.Assert().True(p.GetWorkerVersionCapabilities().GetUseVersioning())
			s.Assert().Equal("bid", p.GetWorkerVersionCapabilities().GetBuildId())
		}
	}
}

func (s *PartitionManagerTestSuite) TestHasAnyPollerAfter() {
	// no pollers
	s.Assert().False(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-5 * time.Minute)))

	// one unversioned poller
	s.pollWithIdentity("uv", "", false)
	s.Assert().True(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-100 * time.Microsecond)))
	time.Sleep(time.Millisecond)
	s.Assert().False(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-100 * time.Microsecond)))

	// one versioned poller
	s.pollWithIdentity("v", "bid", true)
	s.Assert().True(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-100 * time.Microsecond)))
	time.Sleep(time.Millisecond)
	s.Assert().False(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-100 * time.Microsecond)))
}

func (s *PartitionManagerTestSuite) TestHasPollerAfter_Unversioned() {
	// no pollers
	s.Assert().False(s.partitionMgr.HasPollerAfter("", time.Now().Add(-5*time.Minute)))

	// one unversioned poller
	s.pollWithIdentity("uv", "", false)
	s.Assert().True(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-500 * time.Microsecond)))
	s.Assert().True(s.partitionMgr.HasPollerAfter("", time.Now().Add(-500*time.Microsecond)))
	time.Sleep(time.Millisecond)
	s.Assert().False(s.partitionMgr.HasPollerAfter("", time.Now().Add(-100*time.Microsecond)))

	// one versioned poller
	s.pollWithIdentity("v", "bid", true)
	s.Assert().False(s.partitionMgr.HasPollerAfter("", time.Now().Add(-100*time.Microsecond)))
}

func (s *PartitionManagerTestSuite) TestHasPollerAfter_Versioned() {
	// no pollers
	s.Assert().False(s.partitionMgr.HasAnyPollerAfter(time.Now().Add(-5 * time.Minute)))

	// one version-set poller
	bid := "bid"
	s.pollWithIdentity("v", bid, true)
	s.Assert().True(s.partitionMgr.HasPollerAfter(bid, time.Now().Add(-100*time.Microsecond)))
	time.Sleep(time.Millisecond)
	s.Assert().False(s.partitionMgr.HasPollerAfter(bid, time.Now().Add(-100*time.Microsecond)))

	// one unversioned poller
	s.pollWithIdentity("uv", "", false)
	s.Assert().False(s.partitionMgr.HasPollerAfter(bid, time.Now().Add(-100*time.Microsecond)))
}

func (s *PartitionManagerTestSuite) TestLegacyDescribeTaskQueue() {
	// not testing TaskQueueStatus, as it is invalid right now and will be changed with the new LegacyDescribeTaskQueue API
	// no pollers
	pollers := s.partitionMgr.LegacyDescribeTaskQueue(false).DescResponse.GetPollers()
	s.Assert().True(len(pollers) == 0)

	// one unversioned poller
	s.pollWithIdentity("uv", "", false)
	pollers = s.partitionMgr.LegacyDescribeTaskQueue(false).DescResponse.GetPollers()
	s.Assert().True(len(pollers) == 1)

	// one versioned poller
	s.pollWithIdentity("v", "bid", true)
	pollers = s.partitionMgr.LegacyDescribeTaskQueue(false).DescResponse.GetPollers()
	s.Assert().True(len(pollers) == 2)

	for _, p := range pollers {
		switch p.GetIdentity() {
		case "uv":
			s.Assert().False(p.GetWorkerVersionCapabilities().GetUseVersioning())
		case "v":
			s.Assert().True(p.GetWorkerVersionCapabilities().GetUseVersioning())
			s.Assert().Equal("bid", p.GetWorkerVersionCapabilities().GetBuildId())
		}
	}
}

func (s *PartitionManagerTestSuite) validateAddTask(expectedBuildId string, expectedSyncMatch bool, versioningData *persistence.VersioningData, directive *taskqueue.TaskVersionDirective) {
	timeout := 1000000 * time.Millisecond
	if expectedSyncMatch {
		// trySyncMatch "eats" one second from the context timeout!
		timeout += time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	s.userDataMgr.updateVersioningData(versioningData)
	buildId, syncMatch, err := s.partitionMgr.AddTask(ctx, addTaskParams{
		taskInfo: &persistence.TaskInfo{
			NamespaceId:      namespaceId,
			RunId:            "run",
			WorkflowId:       "wf",
			VersionDirective: directive,
		},
		directive: directive,
	})
	s.Assert().NoError(err)
	s.Assert().Equal(expectedSyncMatch, syncMatch)
	s.Assert().Equal(expectedBuildId, buildId)
}

func (s *PartitionManagerTestSuite) validatePollTaskSyncMatch(buildId string, useVersioning bool) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		task, _, err := s.partitionMgr.PollTask(
			ctx, &pollMetadata{
				workerVersionCapabilities: &common.WorkerVersionCapabilities{
					BuildId:       buildId,
					UseVersioning: useVersioning,
				},
			},
		)
		s.Assert().NoError(err)
		s.Assert().NotNil(task)
		s.Assert().NotNil(task.responseC)
		close(task.responseC)
	}()
	// give time for poller to start polling before resuming execution
	time.Sleep(10 * time.Millisecond)
}

// Poll task and assert no error and that a non-nil task is returned
func (s *PartitionManagerTestSuite) validatePollTask(buildId string, useVersioning bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 1000000*time.Millisecond)
	defer cancel()

	task, _, err := s.partitionMgr.PollTask(ctx, &pollMetadata{
		workerVersionCapabilities: &common.WorkerVersionCapabilities{
			BuildId:       buildId,
			UseVersioning: useVersioning,
		},
	})
	s.Assert().NoError(err)
	s.Assert().NotNil(task)
}

// UpdatePollerData is a no-op if the poller context has no identity, so we need a context with identity for any tests that check poller info
func (s *PartitionManagerTestSuite) pollWithIdentity(pollerId, buildId string, useVersioning bool) {
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), identityKey, pollerId), 100*time.Millisecond)
	defer cancel()

	_, _, err := s.partitionMgr.PollTask(ctx, &pollMetadata{
		workerVersionCapabilities: &common.WorkerVersionCapabilities{
			BuildId:       buildId,
			UseVersioning: useVersioning,
		},
	})

	s.True(errors.Is(err, errNoTasks), "no task is expected")
}

func createVersionSet(buildId string) *persistence.CompatibleVersionSet {
	clock := hlc.Zero(1)
	return &persistence.CompatibleVersionSet{
		SetIds: []string{hashBuildId(buildId)},
		BuildIds: []*persistence.BuildId{
			mkBuildId(buildId, clock),
		},
		BecameDefaultTimestamp: clock,
	}
}

type mockUserDataManager struct {
	sync.Mutex
	data *persistence.VersionedTaskQueueUserData
}

func (m *mockUserDataManager) Start() {
	// noop
}

func (m *mockUserDataManager) Stop() {
	// noop
}

func (m *mockUserDataManager) WaitUntilInitialized(_ context.Context) error {
	return nil
}

func (m *mockUserDataManager) GetUserData() (*persistence.VersionedTaskQueueUserData, chan struct{}, error) {
	m.Lock()
	defer m.Unlock()
	return m.data, nil, nil
}

func (m *mockUserDataManager) UpdateUserData(_ context.Context, _ UserDataUpdateOptions, updateFn UserDataUpdateFunc) error {
	m.Lock()
	defer m.Unlock()
	data, _, err := updateFn(m.data.Data)
	if err != nil {
		return err
	}
	m.data = &persistence.VersionedTaskQueueUserData{Data: data, Version: m.data.Version + 1}
	return nil
}

func (m *mockUserDataManager) updateVersioningData(data *persistence.VersioningData) {
	m.Lock()
	defer m.Unlock()
	m.data = &persistence.VersionedTaskQueueUserData{Data: &persistence.TaskQueueUserData{VersioningData: data}}
}

var _ userDataManager = (*mockUserDataManager)(nil)
