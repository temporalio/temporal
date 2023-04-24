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

package history

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/archiver"
)

// TestQueueModule_ArchivalQueueCreated tests that the archival queue is created if and only if the static config for
// either history or visibility archival is enabled.
func TestQueueModule_ArchivalQueue(t *testing.T) {
	for _, c := range []moduleTestCase{
		{
			Name:                "Archival completely disabled",
			HistoryState:        carchiver.ArchivalDisabled,
			VisibilityState:     carchiver.ArchivalDisabled,
			ExpectArchivalQueue: false,
		},
		{
			Name:                "History archival enabled",
			HistoryState:        carchiver.ArchivalEnabled,
			VisibilityState:     carchiver.ArchivalDisabled,
			ExpectArchivalQueue: true,
		},
		{
			Name:                "Visibility archival enabled",
			HistoryState:        carchiver.ArchivalDisabled,
			VisibilityState:     carchiver.ArchivalEnabled,
			ExpectArchivalQueue: true,
		},
		{
			Name:                "Both history and visibility archival enabled",
			HistoryState:        carchiver.ArchivalEnabled,
			VisibilityState:     carchiver.ArchivalEnabled,
			ExpectArchivalQueue: true,
		},
	} {
		c := c
		t.Run(c.Name, c.Run)
	}
}

// moduleTestCase is a test case for the QueueModule.
type moduleTestCase struct {
	Name                string
	HistoryState        carchiver.ArchivalState
	VisibilityState     carchiver.ArchivalState
	ExpectArchivalQueue bool
}

// Run runs the test case.
func (c *moduleTestCase) Run(t *testing.T) {

	controller := gomock.NewController(t)
	dependencies := getModuleDependencies(controller, c)
	var factories []QueueFactory

	app := fx.New(
		dependencies,
		QueueModule,
		fx.Invoke(func(params QueueFactoriesLifetimeHookParams) {
			factories = params.Factories
		}),
	)

	require.NoError(t, app.Err())
	require.NotNil(t, factories)
	var (
		txq QueueFactory
		tiq QueueFactory
		viq QueueFactory
		aq  QueueFactory
	)
	for _, f := range factories {
		switch f.(type) {
		case *transferQueueFactory:
			require.Nil(t, txq)
			txq = f
		case *timerQueueFactory:
			require.Nil(t, tiq)
			tiq = f
		case *visibilityQueueFactory:
			require.Nil(t, viq)
			viq = f
		case *archivalQueueFactory:
			require.Nil(t, aq)
			aq = f
		}
	}
	require.NotNil(t, txq)
	require.NotNil(t, tiq)
	require.NotNil(t, viq)
	if c.ExpectArchivalQueue {
		require.NotNil(t, aq)
		assert.Contains(t, tasks.GetCategories(), tasks.CategoryIDArchival)
	} else {
		require.Nil(t, aq)
		assert.NotContains(t, tasks.GetCategories(), tasks.CategoryIDArchival)
	}
}

// getModuleDependencies returns an fx.Option that provides all the dependencies needed for the queue module.
func getModuleDependencies(controller *gomock.Controller, c *moduleTestCase) fx.Option {
	cfg := configs.NewConfig(
		dynamicconfig.NewNoopCollection(),
		1,
		true,
		false,
	)
	archivalMetadata := getArchivalMetadata(controller, c)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("module-test-cluster-name").AnyTimes()
	return fx.Supply(
		compileTimeDependencies{},
		cfg,
		fx.Annotate(archivalMetadata, fx.As(new(carchiver.ArchivalMetadata))),
		fx.Annotate(metrics.NoopMetricsHandler, fx.As(new(metrics.Handler))),
		fx.Annotate(clusterMetadata, fx.As(new(cluster.Metadata))),
	)
}

// compileTimeDependencies is a struct that provides nil implementations of all the dependencies needed for the queue
// module that are not required for the test at runtime.
type compileTimeDependencies struct {
	fx.Out

	namespace.Registry
	clock.TimeSource
	log.SnTaggedLogger
	client.Bean
	archiver.Client
	sdk.ClientFactory
	resource.MatchingClient
	historyservice.HistoryServiceClient
	manager.VisibilityManager
	archival.Archiver
	workflow.RelocatableAttributesFetcher
}

// getArchivalMetadata returns a mock ArchivalMetadata that contains the static archival config specified in the given
// test case.
func getArchivalMetadata(controller *gomock.Controller, c *moduleTestCase) *carchiver.MockArchivalMetadata {
	archivalMetadata := carchiver.NewMockArchivalMetadata(controller)
	historyConfig := carchiver.NewMockArchivalConfig(controller)
	visibilityConfig := carchiver.NewMockArchivalConfig(controller)
	historyConfig.EXPECT().StaticClusterState().Return(c.HistoryState).AnyTimes()
	visibilityConfig.EXPECT().StaticClusterState().Return(c.VisibilityState).AnyTimes()
	archivalMetadata.EXPECT().GetHistoryConfig().Return(historyConfig).AnyTimes()
	archivalMetadata.EXPECT().GetVisibilityConfig().Return(visibilityConfig).AnyTimes()
	return archivalMetadata
}
