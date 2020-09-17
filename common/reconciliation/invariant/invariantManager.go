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

package invariant

type (
	invariantManager struct {
		invariants []Invariant
	}
)

// NewInvariantManager handles running a collection of invariants according to the invariant collection provided.
func NewInvariantManager(
	invariants []Invariant,
) Manager {
	return &invariantManager{
		invariants: invariants,
	}
}

// RunChecks runs all enabled checks.
func (i *invariantManager) RunChecks(execution interface{}) ManagerCheckResult {
	result := ManagerCheckResult{
		CheckResultType:          CheckResultTypeHealthy,
		DeterminingInvariantType: nil,
		CheckResults:             nil,
	}
	for _, iv := range i.invariants {
		checkResult := iv.Check(execution)
		result.CheckResults = append(result.CheckResults, checkResult)
		checkResultType, updated := i.nextCheckResultType(result.CheckResultType, checkResult.CheckResultType)
		result.CheckResultType = checkResultType
		if updated {
			result.DeterminingInvariantType = &checkResult.InvariantName
		}
	}
	return result
}

// RunFixes runs all enabled fixes.
func (i *invariantManager) RunFixes(execution interface{}) ManagerFixResult {
	result := ManagerFixResult{
		FixResultType:            FixResultTypeSkipped,
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

func (i *invariantManager) nextFixResultType(
	currentState FixResultType,
	event FixResultType,
) (FixResultType, bool) {
	switch currentState {
	case FixResultTypeSkipped:
		return event, event != FixResultTypeSkipped
	case FixResultTypeFixed:
		if event == FixResultTypeFailed {
			return event, true
		}
		return currentState, false
	case FixResultTypeFailed:
		return currentState, false
	default:
		panic("unknown FixResultType")
	}
}

func (i *invariantManager) nextCheckResultType(
	currentState CheckResultType,
	event CheckResultType,
) (CheckResultType, bool) {
	switch currentState {
	case CheckResultTypeHealthy:
		return event, event != CheckResultTypeHealthy
	case CheckResultTypeCorrupted:
		if event == CheckResultTypeFailed {
			return event, true
		}
		return currentState, false
	case CheckResultTypeFailed:
		return currentState, false
	default:
		panic("unknown CheckResultType")
	}
}
