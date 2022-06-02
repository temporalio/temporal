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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	sdkworker "go.temporal.io/sdk/worker"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/service/worker/common"
)

type perNsWorkerManagerSuite struct {
	suite.Suite

	controller      *gomock.Controller
	logger          log.Logger
	cfactory        *sdk.MockClientFactory
	wfactory        *sdk.MockWorkerFactory
	registry        *namespace.MockRegistry
	hostInfo        *membership.HostInfo
	serviceResolver *membership.MockServiceResolver

	cmp1 *common.MockPerNSWorkerComponent
	cmp2 *common.MockPerNSWorkerComponent

	manager *perNamespaceWorkerManager
}

func TestPerNsWorkerManager(t *testing.T) {
	suite.Run(t, new(perNsWorkerManagerSuite))
}

func (s *perNsWorkerManagerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())

	s.logger = log.NewTestLogger()
	s.cfactory = sdk.NewMockClientFactory(s.controller)
	s.wfactory = sdk.NewMockWorkerFactory(s.controller)
	s.registry = namespace.NewMockRegistry(s.controller)
	s.hostInfo = membership.NewHostInfo("self", nil)
	s.serviceResolver = membership.NewMockServiceResolver(s.controller)
	s.cmp1 = common.NewMockPerNSWorkerComponent(s.controller)
	s.cmp2 = common.NewMockPerNSWorkerComponent(s.controller)

	s.manager = NewPerNamespaceWorkerManager(perNamespaceWorkerManagerInitParams{
		Logger:            s.logger,
		SdkClientFactory:  s.cfactory,
		SdkWorkerFactory:  s.wfactory,
		NamespaceRegistry: s.registry,
		HostName:          "self",
		Components:        []common.PerNSWorkerComponent{s.cmp1, s.cmp2},
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

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)
}

func (s *perNsWorkerManagerSuite) TestEnabledButResolvedToOther() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq1",
		NumWorkers: 2,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("other1", nil), nil)
	s.serviceResolver.EXPECT().Lookup("ns1/tq1/1").Return(membership.NewHostInfo("other2", nil), nil)

	s.manager.namespaceCallback(ns, false)
	// main work happens in a goroutine
	time.Sleep(50 * time.Millisecond)
}

func (s *perNsWorkerManagerSuite) TestEnabled() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq1",
		NumWorkers: 1,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("self", nil), nil)
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli1, nil)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.wfactory.EXPECT().New(cli1, "tq1", gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns)
	wkr1.EXPECT().Start()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// shutdown
	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestMultiplicity() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq1",
		NumWorkers: 3,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("self", nil), nil)
	s.serviceResolver.EXPECT().Lookup("ns1/tq1/1").Return(membership.NewHostInfo("other", nil), nil)
	s.serviceResolver.EXPECT().Lookup("ns1/tq1/2").Return(membership.NewHostInfo("self", nil), nil)
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli1, nil)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.wfactory.EXPECT().New(cli1, "tq1", gomock.Any()).Do(func(_, _ any, options sdkworker.Options) {
		s.Equal(4, options.MaxConcurrentWorkflowTaskPollers)
		s.Equal(4, options.MaxConcurrentActivityTaskPollers)
	}).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns)
	wkr1.EXPECT().Start()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestTwoNamespacesTwoComponents() {
	ns1 := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)
	ns2 := testns("ns2", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq1",
		NumWorkers: 1,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq2",
		NumWorkers: 1,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("self", nil), nil)
	s.serviceResolver.EXPECT().Lookup("ns1/tq2/0").Return(membership.NewHostInfo("self", nil), nil)
	s.serviceResolver.EXPECT().Lookup("ns2/tq1/0").Return(membership.NewHostInfo("self", nil), nil)
	s.serviceResolver.EXPECT().Lookup("ns2/tq2/0").Return(membership.NewHostInfo("self", nil), nil)

	cli11 := mocksdk.NewMockClient(s.controller)
	cli12 := mocksdk.NewMockClient(s.controller)
	cli21 := mocksdk.NewMockClient(s.controller)
	cli22 := mocksdk.NewMockClient(s.controller)

	wkr11 := mocksdk.NewMockWorker(s.controller)
	wkr12 := mocksdk.NewMockWorker(s.controller)
	wkr21 := mocksdk.NewMockWorker(s.controller)
	wkr22 := mocksdk.NewMockWorker(s.controller)

	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli11, nil)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli12, nil)
	s.cfactory.EXPECT().NewClient("ns2", gomock.Any()).Return(cli21, nil)
	s.cfactory.EXPECT().NewClient("ns2", gomock.Any()).Return(cli22, nil)

	s.wfactory.EXPECT().New(cli11, "tq1", gomock.Any()).Return(wkr11)
	s.wfactory.EXPECT().New(cli12, "tq2", gomock.Any()).Return(wkr12)
	s.wfactory.EXPECT().New(cli21, "tq1", gomock.Any()).Return(wkr21)
	s.wfactory.EXPECT().New(cli22, "tq2", gomock.Any()).Return(wkr22)

	s.cmp1.EXPECT().Register(wkr11, ns1)
	s.cmp2.EXPECT().Register(wkr12, ns1)
	s.cmp1.EXPECT().Register(wkr21, ns2)
	s.cmp2.EXPECT().Register(wkr22, ns2)

	wkr11.EXPECT().Start()
	wkr12.EXPECT().Start()
	wkr21.EXPECT().Start()
	wkr22.EXPECT().Start()

	s.manager.namespaceCallback(ns1, false)
	s.manager.namespaceCallback(ns2, false)
	time.Sleep(50 * time.Millisecond)

	wkr11.EXPECT().Stop()
	wkr12.EXPECT().Stop()
	wkr21.EXPECT().Stop()
	wkr22.EXPECT().Stop()
	cli11.EXPECT().Close()
	cli12.EXPECT().Close()
	cli21.EXPECT().Close()
	cli22.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestDeleteNs() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq1",
		NumWorkers: 1,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("self", nil), nil)
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli1, nil)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.wfactory.EXPECT().New(cli1, "tq1", gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns)
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
	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("self", nil), nil)
	cli2 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli2, nil)
	wkr2 := mocksdk.NewMockWorker(s.controller)
	s.wfactory.EXPECT().New(cli1, "tq1", gomock.Any()).Return(wkr2)
	s.cmp1.EXPECT().Register(wkr2, ns)
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

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq1",
		NumWorkers: 1,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	// we don't own it at first
	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("other", nil), nil)

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// now we own it
	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("self", nil), nil)
	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli1, nil)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.wfactory.EXPECT().New(cli1, "tq1", gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns)
	wkr1.EXPECT().Start()

	s.manager.membershipChangedCh <- nil
	time.Sleep(50 * time.Millisecond)

	// now we don't own it anymore
	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("other", nil), nil)
	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()

	s.manager.membershipChangedCh <- nil
	time.Sleep(50 * time.Millisecond)
}

func (s *perNsWorkerManagerSuite) TestServiceResolverError() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq1",
		NumWorkers: 1,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(nil, errors.New("resolver error"))
	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(nil, errors.New("resolver error again"))
	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("self", nil), nil)

	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli1, nil)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.wfactory.EXPECT().New(cli1, "tq1", gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns)
	wkr1.EXPECT().Start()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// shutdown
	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestNewClientError() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq1",
		NumWorkers: 1,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("self", nil), nil).AnyTimes()

	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(nil, errors.New("new client error"))
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(nil, errors.New("new client error again"))
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli1, nil)

	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.wfactory.EXPECT().New(cli1, "tq1", gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns)
	wkr1.EXPECT().Start()

	s.manager.namespaceCallback(ns, false)
	time.Sleep(50 * time.Millisecond)

	// shutdown
	wkr1.EXPECT().Stop()
	cli1.EXPECT().Close()
}

func (s *perNsWorkerManagerSuite) TestStartWorkerError() {
	ns := testns("ns1", enumspb.NAMESPACE_STATE_REGISTERED)

	s.cmp1.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled:    true,
		TaskQueue:  "tq1",
		NumWorkers: 1,
	}).AnyTimes()
	s.cmp2.EXPECT().DedicatedWorkerOptions(gomock.Any()).Return(&common.PerNSDedicatedWorkerOptions{
		Enabled: false,
	}).AnyTimes()

	s.serviceResolver.EXPECT().Lookup("ns1/tq1/0").Return(membership.NewHostInfo("self", nil), nil).AnyTimes()

	cli1 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli1, nil)
	wkr1 := mocksdk.NewMockWorker(s.controller)
	s.wfactory.EXPECT().New(cli1, "tq1", gomock.Any()).Return(wkr1)
	s.cmp1.EXPECT().Register(wkr1, ns)

	// first try fails to start
	wkr1.EXPECT().Start().Return(errors.New("start worker error"))
	cli1.EXPECT().Close()

	// second try
	cli2 := mocksdk.NewMockClient(s.controller)
	s.cfactory.EXPECT().NewClient("ns1", gomock.Any()).Return(cli2, nil)
	wkr2 := mocksdk.NewMockWorker(s.controller)
	s.wfactory.EXPECT().New(cli1, "tq1", gomock.Any()).Return(wkr2)
	s.cmp1.EXPECT().Register(wkr2, ns)
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
		"cluster",
	)
}
