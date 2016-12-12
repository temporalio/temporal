package main

import (
	"encoding/json"
	"fmt"

	"code.uber.internal/devexp/minions/test/flow"
	"code.uber.internal/devexp/minions/test/workflow"
)

type (
	// Workflow Deciders and Activities.
	greetingsWorkflow   struct{}
	getNameActivity     struct{}
	getGreetingActivity struct{}
	sayGreetingActivity struct{}

	sayGreetingActivityRequest struct {
		Name     string
		Greeting string
	}
)

// Greetings Workflow Decider.
func (w greetingsWorkflow) Execute(ctx workflow.Context, input []byte) (result []byte, err workflow.Error) {
	// Get Greeting.
	greetResult, err := ctx.ExecuteActivity(activityInfo("getGreetingActivity"))
	if err != nil {
		return nil, err
	}

	// Get Name.
	nameResult, err := ctx.ExecuteActivity(activityInfo("getNameActivity"))
	if err != nil {
		return nil, err
	}

	// Say Greeting.
	request := &sayGreetingActivityRequest{Name: string(nameResult), Greeting: string(greetResult)}
	_, err = ctx.ExecuteActivity(activityInfoWithInput("sayGreetingActivity", request))
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// Get Name Activity.
func (g getNameActivity) Execute(context flow.ActivityExecutionContext, input []byte) ([]byte, flow.Error) {
	var name string
	fmt.Printf("Enter the Name: ")
	fmt.Scanf("%s", &name)
	return []byte(name), nil
}

// Get Greeting Activity.
func (ga getGreetingActivity) Execute(context flow.ActivityExecutionContext, input []byte) ([]byte, flow.Error) {
	var greeting string
	fmt.Printf("Enter the Greeting: ")
	fmt.Scanf("%s", &greeting)
	return []byte(greeting), nil
}

// Say Greeting Activity.
func (ga sayGreetingActivity) Execute(context flow.ActivityExecutionContext, input []byte) ([]byte, flow.Error) {
	greeetingParams := &sayGreetingActivityRequest{}
	err := json.Unmarshal(input, greeetingParams)
	if err != nil {
		return nil, &workflowError{reason: err.Error()}
	}

	fmt.Printf("Saying Final Greeting: ")
	fmt.Printf("%s %s!\n", greeetingParams.Greeting, greeetingParams.Name)
	return nil, nil
}
