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

package queue

import (
	"math/rand"

	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/task"
)

type (
	lookAheadFunc func(task.Key, string) task.Key

	pendingTaskSplitPolicy struct {
		pendingTaskThreshold map[int]int // queue level -> threshold
		maxNewQueueLevel     int
		lookAheadFunc        lookAheadFunc
	}

	stuckTaskSplitPolicy struct {
		attemptThreshold map[int]int // queue level -> threshold
		maxNewQueueLevel int
	}

	selectedDomainSplitPolicy struct {
		domainIDs     map[string]struct{}
		newQueueLevel int
	}

	randomSplitPolicy struct {
		splitProbability  float64
		enabledByDomainID dynamicconfig.BoolPropertyFnWithDomainIDFilter
		maxNewQueueLevel  int
		lookAheadFunc     lookAheadFunc
	}

	aggregatedSplitPolicy struct {
		policies []ProcessingQueueSplitPolicy
	}
)

// NewPendingTaskSplitPolicy creates a new processing queue split policy
// based on the number of pending tasks
func NewPendingTaskSplitPolicy(
	pendingTaskThreshold map[int]int,
	lookAheadFunc lookAheadFunc,
	maxNewQueueLevel int,
) ProcessingQueueSplitPolicy {
	return &pendingTaskSplitPolicy{
		pendingTaskThreshold: pendingTaskThreshold,
		lookAheadFunc:        lookAheadFunc,
		maxNewQueueLevel:     maxNewQueueLevel,
	}
}

// NewStuckTaskSplitPolicy creates a new processing queue split policy
// based on the number of task attempts tasks
func NewStuckTaskSplitPolicy(
	attemptThreshold map[int]int,
	maxNewQueueLevel int,
) ProcessingQueueSplitPolicy {
	return &stuckTaskSplitPolicy{
		attemptThreshold: attemptThreshold,
		maxNewQueueLevel: maxNewQueueLevel,
	}
}

// NewSelectedDomainSplitPolicy creates a new processing queue split policy
// that splits out specific domainIDs
func NewSelectedDomainSplitPolicy(
	domainIDs map[string]struct{},
	newQueueLevel int,
) ProcessingQueueSplitPolicy {
	return &selectedDomainSplitPolicy{
		domainIDs:     domainIDs,
		newQueueLevel: newQueueLevel,
	}
}

// NewRandomSplitPolicy creates a split policy that will randomly split one
// or more domains into a new processing queue
func NewRandomSplitPolicy(
	splitProbability float64,
	enabledByDomainID dynamicconfig.BoolPropertyFnWithDomainIDFilter,
	maxNewQueueLevel int,
	lookAheadFunc lookAheadFunc,
) ProcessingQueueSplitPolicy {
	return &randomSplitPolicy{
		splitProbability:  splitProbability,
		enabledByDomainID: enabledByDomainID,
		maxNewQueueLevel:  maxNewQueueLevel,
		lookAheadFunc:     lookAheadFunc,
	}
}

// NewAggregatedSplitPolicy creates a new processing queue split policy
// that which combines other policies. Policies are evaluated in the order
// they passed in, and if one policy returns an non-empty result, that result
// will be returned as is and policies after that one will not be evaluated
func NewAggregatedSplitPolicy(
	policies ...ProcessingQueueSplitPolicy,
) ProcessingQueueSplitPolicy {
	return &aggregatedSplitPolicy{
		policies: policies,
	}
}

func (p *pendingTaskSplitPolicy) Evaluate(
	queue ProcessingQueue,
) []ProcessingQueueState {
	queueImpl := queue.(*processingQueueImpl)

	if queueImpl.state.level == p.maxNewQueueLevel {
		// already reaches max level, skip splitting
		return nil
	}

	threshold, ok := p.pendingTaskThreshold[queueImpl.state.level]
	if !ok {
		// no threshold specified for the level, skip splitting
		return nil
	}

	pendingTasksPerDomain := make(map[string]int) // domainID -> # of pending tasks
	for _, task := range queueImpl.outstandingTasks {
		pendingTasksPerDomain[task.GetDomainID()]++
	}

	domainToSplit := make(map[string]struct{})
	for domainID, pendingTasks := range pendingTasksPerDomain {
		if pendingTasks > threshold {
			domainToSplit[domainID] = struct{}{}
		}
	}

	return splitQueueHelper(
		queueImpl,
		domainToSplit,
		queueImpl.state.level+1, // split stuck tasks to current level + 1
		p.lookAheadFunc,
	)
}

func (p *stuckTaskSplitPolicy) Evaluate(
	queue ProcessingQueue,
) []ProcessingQueueState {
	queueImpl := queue.(*processingQueueImpl)

	if queueImpl.state.level == p.maxNewQueueLevel {
		// already reaches max level, skip splitting
		return nil
	}

	threshold, ok := p.attemptThreshold[queueImpl.state.level]
	if !ok {
		// no threshold specified for the level, skip splitting
		return nil
	}

	domainToSplit := make(map[string]struct{})
	for _, task := range queueImpl.outstandingTasks {
		domainID := task.GetDomainID()
		attempt := task.GetAttempt()
		if attempt > threshold {
			domainToSplit[domainID] = struct{}{}
		}
	}

	return splitQueueHelper(
		queueImpl,
		domainToSplit,
		queueImpl.state.level+1, // split stuck tasks to current level + 1
		nil,                     // no need to look ahead
	)
}

func (p *selectedDomainSplitPolicy) Evaluate(
	queue ProcessingQueue,
) []ProcessingQueueState {
	domainBelongsToQueue := false
	currentQueueState := queue.State()
	currentDomainFilter := currentQueueState.DomainFilter()
	for domainID := range p.domainIDs {
		if currentDomainFilter.Filter(domainID) {
			domainBelongsToQueue = true
			break
		}
	}

	if !domainBelongsToQueue {
		// no split needed
		return nil
	}

	return []ProcessingQueueState{
		newProcessingQueueState(
			currentQueueState.Level(),
			currentQueueState.AckLevel(),
			currentQueueState.ReadLevel(),
			currentQueueState.MaxLevel(),
			currentDomainFilter.Exclude(p.domainIDs),
		),
		newProcessingQueueState(
			p.newQueueLevel,
			currentQueueState.AckLevel(),
			currentQueueState.ReadLevel(),
			currentQueueState.MaxLevel(),
			// make a copy here so that it won't be accidentally modified when p.domainID is changed
			NewDomainFilter(p.domainIDs, false).copy(),
		),
	}
}

func (p *randomSplitPolicy) Evaluate(
	queue ProcessingQueue,
) []ProcessingQueueState {
	queueImpl := queue.(*processingQueueImpl)

	if queueImpl.state.level == p.maxNewQueueLevel {
		// already reaches max level, skip splitting
		return nil
	}

	domainIDs := make(map[string]struct{})
	for _, task := range queueImpl.outstandingTasks {
		domainIDs[task.GetDomainID()] = struct{}{}
	}

	if len(domainIDs) == 0 {
		return nil
	}

	domainToSplit := make(map[string]struct{})
	for domainID := range domainIDs {
		if !p.enabledByDomainID(domainID) {
			continue
		}
		if !shouldSplit(p.splitProbability) {
			continue
		}

		domainToSplit[domainID] = struct{}{}
	}

	return splitQueueHelper(
		queueImpl,
		domainToSplit,
		queueImpl.state.level+1,
		p.lookAheadFunc,
	)
}

func (p *aggregatedSplitPolicy) Evaluate(
	queue ProcessingQueue,
) []ProcessingQueueState {
	for _, policy := range p.policies {
		newStates := policy.Evaluate(queue)
		if len(newStates) != 0 {
			return newStates
		}
	}
	return nil
}

func splitQueueHelper(
	queueImpl *processingQueueImpl,
	domainToSplit map[string]struct{},
	newQueueLevel int,
	lookAheadFunc lookAheadFunc,
) []ProcessingQueueState {
	if len(domainToSplit) == 0 {
		return nil
	}

	newMaxLevel := queueImpl.state.readLevel
	if lookAheadFunc != nil {
		for domainID := range domainToSplit {
			newMaxLevel = maxTaskKey(newMaxLevel, lookAheadFunc(queueImpl.state.readLevel, domainID))
		}
	}
	if queueImpl.state.maxLevel.Less(newMaxLevel) {
		newMaxLevel = queueImpl.state.maxLevel
	}

	newQueueStates := []ProcessingQueueState{
		newProcessingQueueState(
			newQueueLevel,
			queueImpl.state.ackLevel,
			queueImpl.state.readLevel,
			newMaxLevel,
			NewDomainFilter(domainToSplit, false),
		),
	}

	excludedDomainFilter := queueImpl.state.domainFilter.Exclude(domainToSplit)
	if excludedDomainFilter.ReverseMatch || len(excludedDomainFilter.DomainIDs) != 0 {
		// this means the new domain filter still matches at least one domain
		newQueueStates = append(newQueueStates, newProcessingQueueState(
			queueImpl.state.level,
			queueImpl.state.ackLevel,
			queueImpl.state.readLevel,
			newMaxLevel,
			excludedDomainFilter,
		))
	}

	if !taskKeyEquals(newMaxLevel, queueImpl.state.maxLevel) {
		newQueueStates = append(newQueueStates, newProcessingQueueState(
			queueImpl.state.level,
			newMaxLevel,
			newMaxLevel,
			queueImpl.state.maxLevel,
			queueImpl.state.domainFilter.copy(),
		))
	}

	return newQueueStates
}

func shouldSplit(probability float64) bool {
	if probability <= 0 {
		return false
	}
	if probability >= 1.0 {
		return true
	}
	return rand.Intn(int(1.0/probability)) == 0
}
