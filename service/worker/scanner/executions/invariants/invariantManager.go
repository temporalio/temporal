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
// InvariantManager takes care of ensuring invariants are run in their correct dependency order.
func NewInvariantManager(
	invariantCollections []common.InvariantCollection,
	pr common.PersistenceRetryer,
) common.InvariantManager {
	manager := &invariantManager{}
	manager.invariants, manager.types = getSortedInvariants(invariantCollections, pr)
	return manager
}

// RunChecks runs the Check method of all managed invariants.
// Stops on the first check which indicates corruption or failure.
// Only returns CheckResultTypeHealthy if all managed checks indicate healthy.
func (i *invariantManager) RunChecks(execution common.Execution) common.ManagerCheckResult {
	resources := &common.InvariantResourceBag{}
	var checkResults []common.CheckResult
	for _, iv := range i.invariants {
		checkResult := iv.Check(execution, resources)
		checkResults = append(checkResults, checkResult)
		if checkResult.CheckResultType != common.CheckResultTypeHealthy {
			return common.ManagerCheckResult{
				CheckResultType: checkResult.CheckResultType,
				CheckResults:    checkResults,
			}
		}
	}
	return common.ManagerCheckResult{
		CheckResultType: common.CheckResultTypeHealthy,
		CheckResults:    checkResults,
	}
}

// RunFixes runs the Fix method of all managed invariants.
// Stops on the first fix which indicates a failure.
// Returns FixResultTypeSkipped if all invariants where skipped, if at least one was fixed returns FixResultTypeFixed.
func (i *invariantManager) RunFixes(execution common.Execution) common.ManagerFixResult {
	resources := &common.InvariantResourceBag{}
	encounteredFix := false
	var fixResults []common.FixResult
	for _, iv := range i.invariants {
		fixResult := iv.Fix(execution, resources)
		fixResults = append(fixResults, fixResult)
		if fixResult.FixResultType == common.FixResultTypeFailed {
			return common.ManagerFixResult{
				FixResultType: common.FixResultTypeFailed,
				FixResults:    fixResults,
			}
		}
		if fixResult.FixResultType == common.FixResultTypeFixed {
			encounteredFix = true
		}
	}
	if encounteredFix {
		return common.ManagerFixResult{
			FixResultType: common.FixResultTypeFixed,
			FixResults:    fixResults,
		}
	}
	return common.ManagerFixResult{
		FixResultType: common.FixResultTypeSkipped,
		FixResults:    fixResults,
	}
}

// InvariantTypes returns sorted list of all invariants that manager will run.
func (i *invariantManager) InvariantTypes() []common.InvariantType {
	return i.types
}

func getSortedInvariants(
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
	return []common.Invariant{NewHistoryExists(pr), NewValidFirstEvent(pr)}
}

func getMutableStateCollection(pr common.PersistenceRetryer) []common.Invariant {
	return []common.Invariant{NewOpenCurrentExecution(pr)}
}
