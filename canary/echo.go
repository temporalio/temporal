package canary

import (
	"context"
	"errors"
	"math"
	"reflect"

	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/workflow"
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

func registerEcho(r registrar) {
	registerWorkflow(r, echoWorkflow, wfTypeEcho)
	registerActivity(r, echoActivity, activityTypeEcho)
}

// echoWorkflow is a workflow implementation which simply executes an
// activity that echoes back the input as output
func echoWorkflow(ctx workflow.Context, scheduledTimeNanos int64, namespace string) error {
	profile, err := beginWorkflow(ctx, wfTypeEcho, scheduledTimeNanos)
	if err != nil {
		return err
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
