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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/taskqueue/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
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
	config := NewConfig(dynamicconfig.NewNoopCollection(), false, false)
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

func (s *PartitionManagerTestSuite) TestAddTaskNoRules_NoVersionDirective() {
	s.validateAddTaskBuildId("", nil, nil)
}

func (s *PartitionManagerTestSuite) TestAddTaskNoRules_AssignedTask() {
	s.validateAddTaskBuildId("buildXYZ", nil, &taskqueue.TaskVersionDirective{Value: &taskqueue.TaskVersionDirective_BuildId{BuildId: "buildXYZ"}})
}

func (s *PartitionManagerTestSuite) TestAddTaskNoRules_UnassignedTask() {
	s.validateAddTaskBuildId("", nil, &taskqueue.TaskVersionDirective{Value: &taskqueue.TaskVersionDirective_UseDefault{}})
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_NoVersionDirective() {
	buildId := "bld"
	versioningData := &persistence.VersioningData{AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(buildId)}}
	s.validateAddTaskBuildId("", versioningData, nil)
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_AssignedTask() {
	ruleBld := "rule-bld"
	versioningData := &persistence.VersioningData{AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)}}
	taskBld := "task-bld"
	s.validateAddTaskBuildId(taskBld, versioningData, &taskqueue.TaskVersionDirective{Value: &taskqueue.TaskVersionDirective_BuildId{BuildId: taskBld}})
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRules_UnassignedTask() {
	ruleBld := "rule-bld"
	versioningData := &persistence.VersioningData{AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)}}
	s.validateAddTaskBuildId(ruleBld, versioningData, &taskqueue.TaskVersionDirective{Value: &taskqueue.TaskVersionDirective_UseDefault{}})
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRulesAndVersionSets_NoVersionDirective() {
	ruleBld := "rule-bld"
	vs := createVersionSet("vs-bld")
	versioningData := &persistence.VersioningData{
		AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)},
		VersionSets:     []*persistence.CompatibleVersionSet{vs},
	}

	s.validateAddTaskBuildId("", versioningData, nil)
	// make sure version set queue is not loaded
	s.Assert().Nil(s.partitionMgr.versionedQueues[vs.SetIds[0]])
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRulesAndVersionSets_AssignedTask() {
	ruleBld := "rule-bld"
	vs := createVersionSet("vs-bld")
	versioningData := &persistence.VersioningData{
		AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)},
		VersionSets:     []*persistence.CompatibleVersionSet{vs},
	}

	taskBld := "task-bld"
	s.validateAddTaskBuildId(taskBld, versioningData, &taskqueue.TaskVersionDirective{Value: &taskqueue.TaskVersionDirective_BuildId{BuildId: taskBld}})
	// make sure version set queue is not loaded
	s.Assert().Nil(s.partitionMgr.versionedQueues[vs.SetIds[0]])

	// now use the version set build id
	s.validateAddTaskBuildId("", versioningData, &taskqueue.TaskVersionDirective{Value: &taskqueue.TaskVersionDirective_BuildId{BuildId: vs.BuildIds[0].Id}})
	// make sure version set queue is loaded
	s.Assert().NotNil(s.partitionMgr.versionedQueues[vs.SetIds[0]])
}

func (s *PartitionManagerTestSuite) TestAddTaskWithAssignmentRulesAndVersionSets_UnassignedTask() {
	ruleBld := "rule-bld"
	vs := createVersionSet("vs-bld")
	versioningData := &persistence.VersioningData{
		AssignmentRules: []*persistence.AssignmentRule{createFullAssignmentRule(ruleBld)},
		VersionSets:     []*persistence.CompatibleVersionSet{vs},
	}
	s.validateAddTaskBuildId(ruleBld, versioningData, &taskqueue.TaskVersionDirective{Value: &taskqueue.TaskVersionDirective_UseDefault{}})
	// make sure version set queue is not loaded
	s.Assert().Nil(s.partitionMgr.versionedQueues[vs.SetIds[0]])
}

func (s *PartitionManagerTestSuite) validateAddTaskBuildId(expectedBuildId string, versioningData *persistence.VersioningData, directive *taskqueue.TaskVersionDirective) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	s.userDataMgr.updateVersioningData(versioningData)
	buildId, _, err := s.partitionMgr.AddTask(ctx, addTaskParams{
		taskInfo: &persistence.TaskInfo{
			NamespaceId:      namespaceId,
			RunId:            "run",
			WorkflowId:       "wf",
			VersionDirective: directive,
		},
	})
	s.Assert().NoError(err)
	s.Assert().Equal(expectedBuildId, buildId)
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
