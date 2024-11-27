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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/mock/gomock"
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
			PerNamespaceWorkerCount: func(ns string, cb func(int)) (int, func()) {
				return max(1, map[string]int{"ns1": 1, "ns2": 2, "ns3": 6}[ns]), func() {}
			},
			PerNamespaceWorkerOptions: func(ns string, cb func(sdkworker.Options)) (sdkworker.Options, func()) {
				switch ns {
				case "ns1":
					return sdkworker.Options{
						MaxConcurrentWorkflowTaskPollers: 100,
					}, func() {}
				case "ns2":
					return sdkworker.Options{
						WorkerLocalActivitiesPerSecond: 200.0,
						StickyScheduleToStartTimeout:   7500 * time.Millisecond,
					}, func() {}
				default:
					return sdkworker.Options{}, func() {}
				}
			},
			PerNamespaceWorkerStartRate: dynamicconfig.GetFloatPropertyFn(10),
		},
		Components:      []workercommon.PerNSWorkerComponent{s.cmp1, s.cmp2},
		ClusterMetadata: clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(false, true)),
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

	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("other1")})

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

	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")})
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

/*
		Given machine has ownership of the namespace
		When name space change reported
		Then worker should be started with configuration proportional to administratively set configuration ensuring
	    fair distribution across all the machines owning the namespace
*/
func (s *perNsWorkerManagerSuite) TestMultiplicity() {
	machinesOwningNamespace := []membership.HostInfo{
		membership.NewHostInfoFromAddress("other-1"),
		membership.NewHostInfoFromAddress("other-2"),
		membership.NewHostInfoFromAddress("self"),
	}
	desiredWorkersNumber := 6 //  should match mock configuration in line 88
	expectedMultiplicity := 2

	ns := testns("ns3", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().LookupN("ns3", desiredWorkersNumber).Return(machinesOwningNamespace)
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns3")).Return(cli1)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Do(func(_, _ any, options sdkworker.Options) {
		s.Equal(4, options.MaxConcurrentWorkflowTaskPollers)
		s.Equal(4, options.MaxConcurrentActivityTaskPollers)
		s.Equal(2000, options.MaxConcurrentWorkflowTaskExecutionSize)
		s.Equal(2000, options.MaxConcurrentLocalActivityExecutionSize)
		s.Equal(2000, options.MaxConcurrentActivityExecutionSize)
	}).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns, workercommon.RegistrationDetails{TotalWorkers: desiredWorkersNumber, Multiplicity: expectedMultiplicity})
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

	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("self"),
	}).AnyTimes()
	s.serviceResolver.EXPECT().LookupN("ns2", 2).Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("self"),
	}).AnyTimes()
	s.serviceResolver.EXPECT().LookupN("ns3", 6).Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("self"),
	}).AnyTimes()

	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli1)
	wkr := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Do(func(_, _ any, options sdkworker.Options) {
		s.Equal(100, options.MaxConcurrentWorkflowTaskPollers)
		s.Equal(2, options.MaxConcurrentActivityTaskPollers)
		s.Equal(0.0, options.WorkerLocalActivitiesPerSecond)
	}).Return(wkr)

	cli2 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns2")).Return(cli2)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli2}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Do(func(_, _ any, options sdkworker.Options) {
		s.Equal(4, options.MaxConcurrentWorkflowTaskPollers)
		s.Equal(200.0, options.WorkerLocalActivitiesPerSecond)
		s.Equal(7500*time.Millisecond, options.StickyScheduleToStartTimeout)
	}).Return(wkr)

	cli3 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns3")).Return(cli3)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli3}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Do(func(_, _ any, options sdkworker.Options) {
		s.Equal(12, options.MaxConcurrentWorkflowTaskPollers)
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

	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")})
	s.serviceResolver.EXPECT().LookupN("ns2", 2).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")})

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

	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")})
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
	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")})
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
	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("other")})

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// now we own it
	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")})
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient(matchOptions("ns1")).Return(cli1)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewWorker(matchStrict{cli1}, primitives.PerNSWorkerTaskQueue, gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns, workercommon.RegistrationDetails{TotalWorkers: 1, Multiplicity: 1})
	wkr1.EXPECT().Start()

	s.manager.membershipChangedCh <- nil
	time.Sleep(50 * time.Millisecond)

	// now we don't own it anymore
	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("other")})
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

	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{})
	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{})
	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")})

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

	s.serviceResolver.EXPECT().LookupN("ns1", 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")}).AnyTimes()

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

func (s *perNsWorkerManagerSuite) TestRateLimit() {
	mockLimiter := quotas.NewMockRateLimiter(s.controller)
	s.manager.startLimiter = mockLimiter

	// try to start 100 workers
	// rate limiter will allow 10, then 10 more after a short delay, then no more for 10s
	delay := 50 * time.Millisecond
	for i := 0; i < 100; i++ {
		res := quotas.NewMockReservation(s.controller)
		mockLimiter.EXPECT().Reserve().Return(res)
		switch i % 10 {
		case 2:
			res.EXPECT().Delay().Return(0 * time.Millisecond)
		case 6:
			res.EXPECT().Delay().Return(delay)
		default:
			res.EXPECT().Delay().Return(10 * time.Second)
		}
	}

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{Enabled: true}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&workercommon.PerNSDedicatedWorkerOptions{Enabled: false}).AnyTimes()
	s.cmp1.EXPECT().Register(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	s.serviceResolver.EXPECT().LookupN(gomock.Any(), 1).Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")}).AnyTimes()
	cli := mocksdk.NewMockClient(s.controller)
	wkr := mocksdk.NewMockWorker(s.controller)
	s.cfactory.EXPECT().NewClient(gomock.Any()).Return(cli).AnyTimes()
	s.cfactory.EXPECT().NewWorker(gomock.Any(), gomock.Any(), gomock.Any()).Return(wkr).AnyTimes()
	starts := make(chan struct{}, 100)
	wkr.EXPECT().Start().Do(func() { starts <- struct{}{} }).AnyTimes()

	for i := 0; i < 100; i++ {
		ns := testns(fmt.Sprintf("test-%d", i), enumspb.NAMESPACE_STATE_REGISTERED)
		s.manager.namespaceCallback(ns, false)
	}

	start := time.Now()
	for i := 0; i < 10; i++ {
		<-starts
	}
	select {
	case <-starts:
		s.Fail("should be no more starts yet")
	default:
	}
	s.Less(time.Since(start), delay, "should be 10 started immediately")

	for i := 0; i < 10; i++ {
		<-starts
	}
	select {
	case <-starts:
		s.Fail("should be no more starts yet")
	default:
	}
	s.Greater(time.Since(start), delay, "should be 10 more started later")

	wkr.EXPECT().Stop().AnyTimes()
	cli.EXPECT().Close().AnyTimes()
}

func TestPerNsWorkerManagerSubscription(t *testing.T) {
	// this is separate from the suite for more control over config

	controller := gomock.NewController(t)
	logger := log.NewTestLogger()
	cfactory := sdk.NewMockClientFactory(controller)
	registry := namespace.NewMockRegistry(controller)
	hostInfo := membership.NewHostInfoFromAddress("self")
	serviceResolver := membership.NewMockServiceResolver(controller)
	cmp := workercommon.NewMockPerNSWorkerComponent(controller)
	cli := mocksdk.NewMockClient(controller)
	wkr := mocksdk.NewMockWorker(controller)
	cfactory.EXPECT().NewClient(gomock.Any()).Return(cli).AnyTimes()

	mem := dynamicconfig.NewMemoryClient()
	dc := dynamicconfig.NewCollection(mem, logger)
	dc.Start()
	t.Cleanup(dc.Stop)
	config := NewConfig(dc, nil)

	manager := NewPerNamespaceWorkerManager(perNamespaceWorkerManagerInitParams{
		Logger:            logger,
		SdkClientFactory:  cfactory,
		NamespaceRegistry: registry,
		HostName:          "self",
		Config:            config,
		Components:        []workercommon.PerNSWorkerComponent{cmp},
		ClusterMetadata:   clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(false, true)),
	})
	manager.initialRetry = 1 * time.Millisecond

	registry.EXPECT().RegisterStateChangeCallback(gomock.Any(), gomock.Any())
	serviceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any())

	manager.Start(hostInfo, serviceResolver)

	// set up one namespace
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)
	cmp.EXPECT().DedicatedWorkerOptions(gomock.Any()).
		Return(&workercommon.PerNSDedicatedWorkerOptions{Enabled: true}).
		AnyTimes()
	serviceResolver.EXPECT().LookupN("ns1", 1).
		Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self")}).
		AnyTimes()
	serviceResolver.EXPECT().LookupN("ns1", 2).
		Return([]membership.HostInfo{membership.NewHostInfoFromAddress("self"), membership.NewHostInfoFromAddress("self")}).
		AnyTimes()

	wait := make(chan struct{}, 100)
	expectStop := func() {
		wkr.EXPECT().Stop()
		cli.EXPECT().Close().Do(func() { wait <- struct{}{} })
	}
	expectWorker := func(total, multiplicity, wfpollers int) {
		cfactory.EXPECT().NewWorker(gomock.Any(), primitives.PerNSWorkerTaskQueue, gomock.Any()).
			DoAndReturn(func(_ sdkclient.Client, _ string, options sdkworker.Options) sdkworker.Worker {
				assert.Equal(t, wfpollers, options.MaxConcurrentWorkflowTaskPollers)
				return wkr
			})
		cmp.EXPECT().Register(wkr, ns, workercommon.RegistrationDetails{TotalWorkers: total, Multiplicity: multiplicity})
		wkr.EXPECT().Start().Do(func() { wait <- struct{}{} })
	}

	// register namespace
	expectWorker(1, 1, 2)
	manager.namespaceCallback(ns, false)
	<-wait

	// change worker count to 2: should stop and restart worker with more pollers
	expectStop()
	expectWorker(2, 2, 4)
	revert1 := mem.OverrideSetting(dynamicconfig.WorkerPerNamespaceWorkerCount, 2)
	<-wait
	<-wait

	// change options
	expectStop()
	expectWorker(2, 2, 200)
	revert2 := mem.OverrideSetting(dynamicconfig.WorkerPerNamespaceWorkerOptions, sdkworker.Options{
		MaxConcurrentWorkflowTaskPollers: 100,
	})
	<-wait
	<-wait

	// down to 0 workers
	expectStop()
	revert3 := mem.OverrideSetting(dynamicconfig.WorkerPerNamespaceWorkerCount, 0)
	<-wait

	// back to 2
	expectWorker(2, 2, 200)
	revert3()
	<-wait

	// revert count
	expectStop()
	expectWorker(1, 1, 100)
	revert1()
	<-wait
	<-wait

	// revert options
	expectStop()
	expectWorker(1, 1, 2)
	revert2()
	<-wait
	<-wait

	// stop manager
	expectStop()
	registry.EXPECT().UnregisterStateChangeCallback(gomock.Any())
	serviceResolver.EXPECT().RemoveListener(gomock.Any())
	manager.Stop()
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
