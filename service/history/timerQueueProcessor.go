// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/persistence"
)

type (
	timerQueueProcessorImpl struct {
		currentClusterName     string
		shard                  ShardContext
		activeTimerProcessor   *timerQueueActiveProcessorImpl
		standbyTimerProcessors map[string]*timerQueueStandbyProcessorImpl
	}
)

func newTimerQueueProcessor(shard ShardContext, historyService *historyEngineImpl, executionManager persistence.ExecutionManager, logger bark.Logger) timerQueueProcessor {
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	standbyTimerProcessors := make(map[string]*timerQueueStandbyProcessorImpl)
	for clusterName := range shard.GetService().GetClusterMetadata().GetAllClusterFailoverVersions() {
		if clusterName != currentClusterName {
			standbyTimerProcessors[clusterName] = newTimerQueueStandbyProcessor(shard, historyService, executionManager, clusterName, logger)
		}
	}
	return &timerQueueProcessorImpl{
		currentClusterName:     currentClusterName,
		shard:                  shard,
		activeTimerProcessor:   newTimerQueueActiveProcessor(shard, historyService, executionManager, logger),
		standbyTimerProcessors: standbyTimerProcessors,
	}
}

func (t *timerQueueProcessorImpl) Start() {
	t.activeTimerProcessor.Start()
	for _, standbyTimerProcessor := range t.standbyTimerProcessors {
		standbyTimerProcessor.Start()
	}
}

func (t *timerQueueProcessorImpl) Stop() {
	t.activeTimerProcessor.Stop()
	for _, standbyTimerProcessor := range t.standbyTimerProcessors {
		standbyTimerProcessor.Stop()
	}
}

// NotifyNewTimers - Notify the processor about the new active / standby timer arrival.
// This should be called each time new timer arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueProcessorImpl) NotifyNewTimers(clusterName string, timerTasks []persistence.Task) {
	if clusterName == t.currentClusterName {
		t.activeTimerProcessor.notifyNewTimers(timerTasks)
	} else {
		standbyTimerProcessor, ok := t.standbyTimerProcessors[clusterName]
		if !ok {
			panic(fmt.Sprintf("Cannot find timer processot for %s.", clusterName))
		}
		standbyTimerProcessor.notifyNewTimers(timerTasks)
	}
}

func (t *timerQueueProcessorImpl) SetCurrentTime(clusterName string, currentTime time.Time) {
	if clusterName == t.currentClusterName {
		panic(fmt.Sprintf("Cannot change current time of current cluster: %s.", clusterName))
	}

	standbyTimerProcessor, ok := t.standbyTimerProcessors[clusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot find timer processot for %s.", clusterName))
	}
	standbyTimerProcessor.setCurrentTime(currentTime)
}

func (t *timerQueueProcessorImpl) getTimerFiredCount(clusterName string) uint64 {
	if clusterName == t.currentClusterName {
		return t.activeTimerProcessor.getTimerFiredCount()
	}

	standbyTimerProcessor, ok := t.standbyTimerProcessors[clusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot find timer processot for %s.", clusterName))
	}
	return standbyTimerProcessor.getTimerFiredCount()
}
