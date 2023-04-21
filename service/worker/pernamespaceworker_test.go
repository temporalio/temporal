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

package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/common/util"
	workercommon "go.temporal.io/server/service/worker/common"
)

type perNsWorkerManagerSuite struct {
	suite.Suite

	controller      *gomock.Controller
	logger          log.Logger
	cfactory        *sdk.MockClientFactory
	registry        *namespace.MockRegistry
	hostInfo        membership.HostInfo
	serviceResolver *membership.MockServiceResolver

	cmp1 *workercommon.MockPerNSWorkerComponent
	cmp2 *workercommon.MockPerNSWorkerComponent

	manager *perNamespaceWorkerManager
}

func TestPerNsWorkerManager(t *testing.T) {
	suite.Run(t, new(perNsWorkerManagerSuite))
}

func (s *perNsWorkerManagerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())

	s.logger = log.NewTestLogger()
	s.cfactory = sdk.NewMockClientFactory(s.controller)
	s.registry = namespace.NewMockRegistry(s.controller)
	s.hostInfo = membership.NewHostInfoFromAddress("self")
	s.serviceResolver = membership.NewMockServiceResolver(s.controller)
	s.cmp1 = workercommon.NewMockPerNSWorkerComponent(s.controller)
	s.cmp2 = workercommon.NewMockPerNSWorkerComponent(s.controller)

	s.manager = NewPerNamespaceWorkerManager(perNamespaceWorkerManagerInitParams{
		Logger:            s.logger,
		SdkClientFactory:  s.cfactory,
		NamespaceRegistry: s.registry,
		HostName:          "self",
		Config: &Config{
			PerNamespaceWorkerCount: func(ns string) int {
				return util.Max(1, map[string]int{"ns1": 1, "ns2": 2, "ns3": 3}[ns])
			},
			PerNamespaceWorkerOptions: func(ns string) map[string]any {
				switch ns {
				case "ns1":
					return map[string]any{
						"MaxConcurrentWorkflowTaskPollers": 100,
					}
				case "ns2":
					return map[string]any{
						"WorkerLocalActivitiesPerSecond": 200.0,
						"StickyScheduleToStartTimeout":   "7.5s",
					}
				default:
					return map[string]any{}
				}
			},
		},
		Components:      []workercommon.PerNSWorkerComponent{s.cmp1, s.cmp2},
		ClusterMetadata: cluster.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(false, true)),
	})
	s.manager.initialRetry = 1 * time.Millisecond

	s.registry.EXPECT().RegisterStateChangeCallback(gomock.Any(), gomock.Any())
	s.serviceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any())

	s.manager.Start(s.hostInfo, s.serviceResolver)
}

func (s *perNsWorkerManagerSuite) TearDownTest() {
	s.registry.EXPECT().UnregisterStateChangeCallback(gomock.Any())
	s.serviceResolver.EXPECT().RemoveListener(gomock.Any())
	s.manager.Stop()
	s.controller.Finish()
}

func (s *perNsWorkerManagerSuite) TestDisabled() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)
}

func (s *perNsWorkerManagerSuite) TestInactive() {
	ns := testInactiveNS("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)
}

func (s *perNsWorkerManagerSuite) TestEnabledButResolvedToOther() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("other1"), nil)

	s.manager.namespaceCallback(ns, false)
	// main work happens in a goroutine
	time.Sleep(50 * time.Millisecond)
}

func (s *perNsWorkerManagerSuite) TestEnabled() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("self"), nil)
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli1)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})
	wkr1.EXPECT().Start()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// shutdown
	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestMultiplicity() {
	ns := testns("ns3", enumspb.NAMESPACE_STATE_REGISTERED) // three workers

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns3/0").Return(membership.NewHostInfoFromAddress("self"), nil)
	s.serviceResolver.EXPECT().Lookup("ns3/1").Return(membership.NewHostInfoFromAddress("other"), nil)
	s.serviceResolver.EXPECT().Lookup("ns3/2").Return(membership.NewHostInfoFromAddress("self"), nil)
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns3")).Return(cli1)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Do(func(_, _ any, options sdkworker.Options) {
		s.Equal(4, options.MaxConcurrentWorkflowTaskPollers)
		s.Equal(4, options.MaxConcurrentActivityTaskPollers)
	}).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns, workercommon.RegistrationDetails{TotalWorkers: 3, Multiplicity: 2})
	wkr1.EXPECT().Start()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestOptions() {
	ns1 := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)
	ns2 := testns("ns2", enumspb.NAMESPACE_STATE_REGISTERED)
	ns3 := testns("ns3", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup(gomock.Any()).Return(membership.NewHostInfoFromAddress("self"), nil).AnyTimes()
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli1)
	cli2 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns2")).Return(cli2)
	cli3 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns3")).Return(cli3)
	wkr := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Do(func(_, _ any, options sdkworker.Options) {
		s.Equal(100, options.MaxConcurrentWorkflowTaskPollers)
		s.Equal(2, options.MaxConcurrentActivityTaskPollers)
		s.Equal(0.0, options.WorkerLocalActivitiesPerSecond)
	}).Return(wkr)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli2}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Do(func(_, _ any, options sdkworker.Options) {
		s.Equal(4, options.MaxConcurrentWorkflowTaskPollers)
		s.Equal(200.0, options.WorkerLocalActivitiesPerSecond)
		s.Equal(7500*time.Millisecond, options.StickyScheduleToStartTimeout)
	}).Return(wkr)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli3}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Do(func(_, _ any, options sdkworker.Options) {
		s.Equal(6, options.MaxConcurrentWorkflowTaskPollers)
		s.Equal(0.0, options.WorkerLocalActivitiesPerSecond)
		s.Equal(0*time.Millisecond, options.StickyScheduleToStartTimeout)
	}).Return(wkr)
	s.cmp1.EXPECT().Register(wkr, gomock.Any(), gomock.Any()).AnyTimes()
	wkr.EXPECT().Start().AnyTimes()

	s.manager.namespaceCallback(ns1, false)
	s.manager.namespaceCallback(ns2, false)
	s.manager.namespaceCallback(ns3, false)
	time.Sleep(50 * time.Millisecond)

	wkr.EXPECT().Stop().AnyTimes()
	cli1.EXPECT().Close()
	cli2.EXPECT().Close()
	cli3.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestTwoNamespacesTwoComponents() {
	ns1 := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)
	ns2 := testns("ns2", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).DoAndReturn(
		func(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
			return &workercommon.PerNSDedicatedWorkerOptions{Enabled: true}
		}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).DoAndReturn(
		func(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
			// only enabled on ns1
			return &workercommon.PerNSDedicatedWorkerOptions{Enabled: ns.Name().String() == "ns1"}
		}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("self"), nil)
	s.serviceResolver.EXPECT().Lookup("ns2/0").Return(membership.NewHostInfoFromAddress("self"), nil)
	s.serviceResolver.EXPECT().Lookup("ns2/1").Return(membership.NewHostInfoFromAddress("self"), nil)

	cli1 := mocksdk.NewMockClient(s.controller)
	cli2 := mocksdk.NewMockClient(s.controller)

	wkr1 := mocksdk.NewMockWorker(s.controller)
	wkr2 := mocksdk.NewMockWorker(s.controller)

	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli1)
	s.cfactory.EXPECT().NewClient(matchOptions("ns2")).Return(cli2)

	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr1)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli2}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr2)

	s.cmp1.EXPECT().Register(wkr1, ns1, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})
	s.cmp1.EXPECT().Register(wkr2, ns2, workercommon.RegistrationDetails{TotalWorkers: 2, Multiplicity: 2})
	s.cmp2.EXPECT().Register(wkr1, ns1, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})

	wkr1.EXPECT().Start()
	wkr2.EXPECT().Start()

	s.manager.namespaceCallback(ns1, false)
	s.manager.namespaceCallback(ns2, false)
	time.Sleep(50 * time.Millisecond)

	wkr1.EXPECT().Stop()
	wkr2.EXPECT().Stop()
	cli1.EXPECT().Close()
	cli2.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestDeleteNs() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("self"), nil)
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli1)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})
	wkr1.EXPECT().Start()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// now delete it
	nsDeleted := testns("ns1", enumspb.NAMESPACE_STATE_DELETED)
	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()
	s.manager.namespaceCallback(nsDeleted, false)
	time.Sleep(50 * time.Millisecond)

	// restore it
	nsRestored := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)
	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("self"), nil)
	cli2 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli2)
	wkr2 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli2}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr2)
	s.cmp1.EXPECT().Register(wkr2, ns, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})
	wkr2.EXPECT().Start()

	s.manager.namespaceCallback(nsRestored, false)
	time.Sleep(50 * time.Millisecond)

	// now delete from db without changing state
	wkr2.EXPECT().Stop()
	cli2.EXPECT().Close()
	s.manager.namespaceCallback(nsRestored, true)
	time.Sleep(50 * time.Millisecond)
}

func (s *perNsWorkerManagerSuite) TestMembershipChanged() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	// we don't own it at first
	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("other"), nil)

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// now we own it
	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("self"), nil)
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli1)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})
	wkr1.EXPECT().Start()

	s.manager.membershipChangedCh <- nil
	time.Sleep(50 * time.Millisecond)

	// now we don't own it anymore
	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("other"), nil)
	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()

	s.manager.membershipChangedCh <- nil
	time.Sleep(50 * time.Millisecond)
}

func (s *perNsWorkerManagerSuite) TestServiceResolverError() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(nil, errors.New("resolver error"))
	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(nil, errors.New("resolver error again"))
	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("self"), nil)

	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli1)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})
	wkr1.EXPECT().Start()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// shutdown
	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestStartWorkerError() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/0").Return(membership.NewHostInfoFromAddress("self"), nil).AnyTimes()

	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli1)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})

	// first try fails to start
	wkr1.EXPECT().Start().Return(errors.New("start worker error"))
	cli1.EXPECT().Close()

	// second try
	cli2 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli2)
	wkr2 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli2}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr2)
	s.cmp1.EXPECT().Register(wkr2, ns, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})
	wkr2.EXPECT().Start()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// shutdown
	wkr2.EXPECT().Stop()
	cli2.EXPECT().Close()
}

func testns(name string, state enumspb.NamespaceState) *namespace.Namespace {
	return namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:    name,
			State: state,
			Name:  name,
		},
		nil,
		cluster.TestCurrentClusterName,
	)
}

func testInactiveNS(name string, state enumspb.NamespaceState) *namespace.Namespace {
	return namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:    name,
			State: state,
			Name:  name,
		},
		nil,
		true,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName, // not active
			Clusters:          cluster.TestAllClusterNames,
		},
		0,
	)
}

type matchOptions string

func (ns matchOptions) Matches(v any) bool {
	options, ok := v.(sdkclient.Options)
	return ok && options.Namespace == string(ns)
}

func (ns matchOptions) String() string {
	return fmt.Sprintf("namespace=%q", string(ns))
}

// matchStrict implements shallow matching for gomock (default uses reflect.DeepEqual)
type matchStrict struct{ v any }

func (m matchStrict) Matches(v any) bool { return v == m.v }
func (m matchStrict) String() string     { return fmt.Sprintf("%#v", m) }
