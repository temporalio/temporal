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
	"fmt"
	"math/rand"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
	t "github.com/uber/cadence/common/task"
	"github.com/uber/cadence/service/history/task"
)

const (
	policyTypePendingTask int = iota + 1
	policyTypeStuckTask
	policyTypeSelectedDomain
	policyTypeRandom
)

type (
	lookAheadFunc func(task.Key, string) task.Key

	pendingTaskSplitPolicy struct {
		pendingTaskThreshold map[int]int // queue level -> threshold
		enabledByDomainID    dynamicconfig.BoolPropertyFnWithDomainIDFilter
		maxNewQueueLevel     int
		lookAheadFunc        lookAheadFunc

		logger       log.Logger
		metricsScope metrics.Scope
	}

	stuckTaskSplitPolicy struct {
		attemptThreshold  map[int]int // queue level -> threshold
		enabledByDomainID dynamicconfig.BoolPropertyFnWithDomainIDFilter
		maxNewQueueLevel  int

		logger       log.Logger
		metricsScope metrics.Scope
	}

	selectedDomainSplitPolicy struct {
		domainIDs     map[string]struct{}
		newQueueLevel int

		logger       log.Logger
		metricsScope metrics.Scope
	}

	randomSplitPolicy struct {
		splitProbability  float64
		enabledByDomainID dynamicconfig.BoolPropertyFnWithDomainIDFilter
		maxNewQueueLevel  int
		lookAheadFunc     lookAheadFunc

		logger       log.Logger
		metricsScope metrics.Scope
	}

	aggregatedSplitPolicy struct {
		policies []ProcessingQueueSplitPolicy
	}
)

// NewPendingTaskSplitPolicy creates a new processing queue split policy
// based on the number of pending tasks
func NewPendingTaskSplitPolicy(
	pendingTaskThreshold map[int]int,
	enabledByDomainID dynamicconfig.BoolPropertyFnWithDomainIDFilter,
	lookAheadFunc lookAheadFunc,
	maxNewQueueLevel int,
	logger log.Logger,
	metricsScope metrics.Scope,
) ProcessingQueueSplitPolicy {
	return &pendingTaskSplitPolicy{
		pendingTaskThreshold: pendingTaskThreshold,
		enabledByDomainID:    enabledByDomainID,
		lookAheadFunc:        lookAheadFunc,
		maxNewQueueLevel:     maxNewQueueLevel,
		logger:               logger,
		metricsScope:         metricsScope,
	}
}

// NewStuckTaskSplitPolicy creates a new processing queue split policy
// based on the number of task attempts tasks
func NewStuckTaskSplitPolicy(
	attemptThreshold map[int]int,
	enabledByDomainID dynamicconfig.BoolPropertyFnWithDomainIDFilter,
	maxNewQueueLevel int,
	logger log.Logger,
	metricsScope metrics.Scope,
) ProcessingQueueSplitPolicy {
	return &stuckTaskSplitPolicy{
		attemptThreshold:  attemptThreshold,
		enabledByDomainID: enabledByDomainID,
		maxNewQueueLevel:  maxNewQueueLevel,
		logger:            logger,
		metricsScope:      metricsScope,
	}
}

// NewSelectedDomainSplitPolicy creates a new processing queue split policy
// that splits out specific domainIDs
func NewSelectedDomainSplitPolicy(
	domainIDs map[string]struct{},
	newQueueLevel int,
	logger log.Logger,
	metricsScope metrics.Scope,
) ProcessingQueueSplitPolicy {
	return &selectedDomainSplitPolicy{
		domainIDs:     domainIDs,
		newQueueLevel: newQueueLevel,
		logger:        logger,
		metricsScope:  metricsScope,
	}
}

// NewRandomSplitPolicy creates a split policy that will randomly split one
// or more domains into a new processing queue
func NewRandomSplitPolicy(
	splitProbability float64,
	enabledByDomainID dynamicconfig.BoolPropertyFnWithDomainIDFilter,
	maxNewQueueLevel int,
	lookAheadFunc lookAheadFunc,
	logger log.Logger,
	metricsScope metrics.Scope,
) ProcessingQueueSplitPolicy {
	return &randomSplitPolicy{
		splitProbability:  splitProbability,
		enabledByDomainID: enabledByDomainID,
		maxNewQueueLevel:  maxNewQueueLevel,
		lookAheadFunc:     lookAheadFunc,
		logger:            logger,
		metricsScope:      metricsScope,
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
		if task.State() != t.TaskStateAcked {
			pendingTasksPerDomain[task.GetDomainID()]++
		}
	}

	domainToSplit := make(map[string]struct{})
	for domainID, pendingTasks := range pendingTasksPerDomain {
		if pendingTasks > threshold && p.enabledByDomainID(domainID) {
			domainToSplit[domainID] = struct{}{}
		}
	}

	if len(domainToSplit) == 0 {
		return nil
	}

	newQueueLevel := queueImpl.state.level + 1 // split stuck tasks to current level + 1
	p.logger.Info("Split processing queue",
		tag.QueueLevel(newQueueLevel),
		tag.PreviousQueueLevel(queueImpl.state.level),
		tag.WorkflowDomainIDs(domainToSplit),
		tag.QueueSplitPolicyType(policyTypePendingTask),
	)
	p.metricsScope.IncCounter(metrics.ProcessingQueuePendingTaskSplitCounter)

	return splitQueueHelper(
		queueImpl,
		domainToSplit,
		newQueueLevel,
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
		if attempt > threshold && p.enabledByDomainID(domainID) {
			domainToSplit[domainID] = struct{}{}
		}
	}

	if len(domainToSplit) == 0 {
		return nil
	}

	newQueueLevel := queueImpl.state.level + 1 // split stuck tasks to current level + 1
	p.logger.Info("Split processing queue",
		tag.QueueLevel(newQueueLevel),
		tag.PreviousQueueLevel(queueImpl.state.level),
		tag.WorkflowDomainIDs(domainToSplit),
		tag.QueueSplitPolicyType(policyTypeStuckTask),
	)
	p.metricsScope.IncCounter(metrics.ProcessingQueueStuckTaskSplitCounter)

	return splitQueueHelper(
		queueImpl,
		domainToSplit,
		newQueueLevel,
		nil, // no need to look ahead
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

	p.logger.Info("Split processing queue",
		tag.QueueLevel(currentQueueState.Level()),
		tag.PreviousQueueLevel(p.newQueueLevel),
		tag.WorkflowDomainIDs(p.domainIDs),
		tag.QueueSplitPolicyType(policyTypeSelectedDomain),
	)
	p.metricsScope.IncCounter(metrics.ProcessingQueueSelectedDomainSplitCounter)

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

	if len(domainToSplit) == 0 {
		return nil
	}

	newQueueLevel := queueImpl.state.level + 1 // split stuck tasks to current level + 1
	p.logger.Info("Split processing queue",
		tag.QueueLevel(newQueueLevel),
		tag.PreviousQueueLevel(queueImpl.state.level),
		tag.WorkflowDomainIDs(domainToSplit),
		tag.QueueSplitPolicyType(policyTypeRandom),
	)
	p.metricsScope.IncCounter(metrics.ProcessingQueueRandomSplitCounter)

	return splitQueueHelper(
		queueImpl,
		domainToSplit,
		newQueueLevel,
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

// splitQueueHelper assumes domainToSplit is not empty
func splitQueueHelper(
	queueImpl *processingQueueImpl,
	domainToSplit map[string]struct{},
	newQueueLevel int,
	lookAheadFunc lookAheadFunc,
) []ProcessingQueueState {
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

	for _, state := range newQueueStates {
		if state.ReadLevel().Less(state.AckLevel()) || state.MaxLevel().Less(state.ReadLevel()) {
			panic(fmt.Sprintf("invalid processing queue split result: %v, state before split: %v, newMaxLevel: %v", state, queueImpl.state, newMaxLevel))
		}
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
