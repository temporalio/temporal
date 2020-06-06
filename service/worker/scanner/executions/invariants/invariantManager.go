// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package invariants

import (
	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

type (
	invariantManager struct {
		invariants []common.Invariant
		types      []common.InvariantType
	}
)

// NewInvariantManager handles running a collection of invariants according to the invariant collection provided.
func NewInvariantManager(
	invariantCollections []common.InvariantCollection,
	pr common.PersistenceRetryer,
) common.InvariantManager {
	manager := &invariantManager{}
	manager.invariants, manager.types = flattenInvariants(invariantCollections, pr)
	return manager
}

// RunChecks runs all enabled checks.
func (i *invariantManager) RunChecks(execution common.Execution) common.ManagerCheckResult {
	result := common.ManagerCheckResult{
		CheckResultType:          common.CheckResultTypeHealthy,
		DeterminingInvariantType: nil,
		CheckResults:             nil,
	}
	for _, iv := range i.invariants {
		checkResult := iv.Check(execution)
		result.CheckResults = append(result.CheckResults, checkResult)
		checkResultType, updated := i.nextCheckResultType(result.CheckResultType, checkResult.CheckResultType)
		result.CheckResultType = checkResultType
		if updated {
			result.DeterminingInvariantType = &checkResult.InvariantType
		}
	}
	return result
}

// RunFixes runs all enabled fixes.
func (i *invariantManager) RunFixes(execution common.Execution) common.ManagerFixResult {
	result := common.ManagerFixResult{
		FixResultType:            common.FixResultTypeSkipped,
		DeterminingInvariantType: nil,
		FixResults:               nil,
	}
	for _, iv := range i.invariants {
		fixResult := iv.Fix(execution)
		result.FixResults = append(result.FixResults, fixResult)
		fixResultType, updated := i.nextFixResultType(result.FixResultType, fixResult.FixResultType)
		result.FixResultType = fixResultType
		if updated {
			result.DeterminingInvariantType = &fixResult.InvariantType
		}
	}
	return result
}

// InvariantTypes returns sorted list of all invariants that manager will run.
func (i *invariantManager) InvariantTypes() []common.InvariantType {
	return i.types
}

func (i *invariantManager) nextFixResultType(
	currentState common.FixResultType,
	event common.FixResultType,
) (common.FixResultType, bool) {
	switch currentState {
	case common.FixResultTypeSkipped:
		return event, event != common.FixResultTypeSkipped
	case common.FixResultTypeFixed:
		if event == common.FixResultTypeFailed {
			return event, true
		}
		return currentState, false
	case common.FixResultTypeFailed:
		return currentState, false
	default:
		panic("unknown FixResultType")
	}
}

func (i *invariantManager) nextCheckResultType(
	currentState common.CheckResultType,
	event common.CheckResultType,
) (common.CheckResultType, bool) {
	switch currentState {
	case common.CheckResultTypeHealthy:
		return event, event != common.CheckResultTypeHealthy
	case common.CheckResultTypeCorrupted:
		if event == common.CheckResultTypeFailed {
			return event, true
		}
		return currentState, false
	case common.CheckResultTypeFailed:
		return currentState, false
	default:
		panic("unknown CheckResultType")
	}
}

func flattenInvariants(
	collections []common.InvariantCollection,
	pr common.PersistenceRetryer,
) ([]common.Invariant, []common.InvariantType) {
	var ivs []common.Invariant
	for _, collection := range collections {
		switch collection {
		case common.InvariantCollectionHistory:
			ivs = append(ivs, getHistoryCollection(pr)...)
		case common.InvariantCollectionMutableState:
			ivs = append(ivs, getMutableStateCollection(pr)...)
		}
	}
	types := make([]common.InvariantType, len(ivs), len(ivs))
	for i, iv := range ivs {
		types[i] = iv.InvariantType()
	}
	return ivs, types
}

func getHistoryCollection(pr common.PersistenceRetryer) []common.Invariant {
	return []common.Invariant{NewHistoryExists(pr)}
}

func getMutableStateCollection(pr common.PersistenceRetryer) []common.Invariant {
	return []common.Invariant{NewOpenCurrentExecution(pr)}
}
