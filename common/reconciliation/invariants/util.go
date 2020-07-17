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
	"github.com/uber/cadence/common/reconciliation/common"
)

func checkBeforeFix(
	invariant common.Invariant,
	execution common.Execution,
) (*common.FixResult, *common.CheckResult) {
	checkResult := invariant.Check(execution)
	if checkResult.CheckResultType == common.CheckResultTypeHealthy {
		return &common.FixResult{
			FixResultType: common.FixResultTypeSkipped,
			InvariantType: invariant.InvariantType(),
			CheckResult:   checkResult,
			Info:          "skipped fix because execution was healthy",
		}, nil
	}
	if checkResult.CheckResultType == common.CheckResultTypeFailed {
		return &common.FixResult{
			FixResultType: common.FixResultTypeFailed,
			InvariantType: invariant.InvariantType(),
			CheckResult:   checkResult,
			Info:          "failed fix because check failed",
		}, nil
	}
	return nil, &checkResult
}
