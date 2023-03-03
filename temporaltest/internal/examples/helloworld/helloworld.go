// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package helloworld

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Greet implements a Temporal workflow that returns a salutation for a given subject.
func Greet(ctx workflow.Context, subject string) (string, error) {
	var greeting string
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: time.Second}),
		PickGreeting,
	).Get(ctx, &greeting); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s %s", greeting, subject), nil
}

// PickGreeting is a Temporal activity that returns some greeting text.
func PickGreeting(ctx context.Context) (string, error) {
	return "Hello", nil
}

func TestIntercept(ctx context.Context) (string, error) {
	return "Ok", nil
}

func RegisterWorkflowsAndActivities(r worker.Registry) {
	r.RegisterWorkflow(Greet)
	r.RegisterActivity(PickGreeting)
	r.RegisterActivityWithOptions(TestIntercept, activity.RegisterOptions{Name: "TestIntercept"})
}
