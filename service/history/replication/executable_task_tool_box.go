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

package replication

import (
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.uber.org/fx"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

type (
	ProcessToolBox struct {
		fx.In

		Config                    *configs.Config
		ClusterMetadata           cluster.Metadata
		ClientBean                client.Bean
		ShardController           shard.Controller
		NamespaceCache            namespace.Registry
		EagerNamespaceRefresher   EagerNamespaceRefresher
		NDCHistoryResender        xdc.NDCHistoryResender
		HighPriorityTaskScheduler ctasks.Scheduler[TrackableExecutableTask] `name:"HighPriorityTaskScheduler"`
		// consider using a single TaskScheduler i.e. InterleavedWeightedRoundRobinScheduler instead of two
		LowPriorityTaskScheduler ctasks.Scheduler[TrackableExecutableTask] `name:"LowPriorityTaskScheduler"`
		MetricsHandler           metrics.Handler
		Logger                   log.Logger
		EventSerializer          serialization.Serializer
		DLQWriter                DLQWriter
		HistoryEventsHandler     eventhandler.HistoryEventsHandler
	}
)
