// Copyright (c) 2019 Uber Technologies, Inc.
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

package canary

import (
	"context"
	"errors"
	"math"
	"reflect"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

// echoInput is the input to echoActivity
type echoInput struct {
	IntVal       int64
	IntPtrVal    *int64
	FloatVal     float64
	StringVal    string
	StringPtrVal *string
	SliceVal     []string
	MapVal       map[string]string
}

// echoOutput is the output from echoActivity
type echoOutput echoInput

func init() {
	registerWorkflow(echoWorkflow, wfTypeEcho)
	registerActivity(echoActivity, activityTypeEcho)
}

// echoWorkflow is a workflow implementation which simply executes an
// activity that echoes back the input as output
func echoWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	profile, err := beginWorkflow(ctx, wfTypeEcho, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	input := newEchoInput()
	now := workflow.Now(ctx).UnixNano()
	aCtx := workflow.WithActivityOptions(ctx, newActivityOptions())
	future := workflow.ExecuteActivity(aCtx, activityTypeEcho, now, input)

	var output echoOutput
	if err := future.Get(aCtx, &output); err != nil {
		workflow.GetLogger(ctx).Info("echoActivity failed", zap.Error(err))
		return profile.end(err)
	}

	if !reflect.DeepEqual(output, echoOutput(*input)) {
		workflow.GetLogger(ctx).Sugar().Error("echoActivity returned wrong output", output, input)
		return profile.end(errors.New("echoActivity returned invalid output"))
	}

	return profile.end(nil)
}

// echoActivity is an activity implementation that simply returns the input as output
// it also validates that the passed in input has the exepected values
func echoActivity(ctx context.Context, scheduledTimeNanos int64, input *echoInput) (echoOutput, error) {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeEcho, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)
	expected := newEchoInput()
	if !reflect.DeepEqual(expected, input) {
		err = errors.New("equality check failed")
		return echoOutput{}, err
	}
	return echoOutput(*input), nil
}

func newEchoInput() *echoInput {
	intVal := int64(math.MaxInt64)
	stringVal := "canary_echo_test"
	return &echoInput{
		IntVal:       intVal,
		IntPtrVal:    &intVal,
		FloatVal:     math.MaxFloat64,
		StringVal:    stringVal,
		StringPtrVal: &stringVal,
		SliceVal:     []string{"canary", ".", "echoWorkflow"},
		MapVal:       map[string]string{"us-east-1": "dca1a", "us-west-1": "sjc1a"},
	}
}
