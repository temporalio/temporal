package main

import (
	"encoding/json"
	"fmt"

	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/test/flow"
	"code.uber.internal/devexp/minions/test/workflow"
)

type (
	greetingsWorkflow struct {
	}

	getNameActivity struct {
	}

	getGreetingActivity struct {
	}

	sayGreetingActivity struct {
	}

	sayGreetingActivityRequest struct {
		Name     string
		Greeting string
	}

	workflowError struct {
		reason  string
		details []byte
	}
)

func serializeParams(activityName string, input []byte) flow.ExecuteActivityParameters {
	return flow.ExecuteActivityParameters{
		TaskListName: "testTaskList",
		ActivityType: m.ActivityType{Name: common.StringPtr(activityName)},
		Input:        input}
}

func (we *workflowError) Reason() string {
	return we.reason
}

func (we *workflowError) Details() []byte {
	return we.details
}

func (we *workflowError) Error() string {
	return we.reason
}

// Greetings Workflow Decider.
func (w greetingsWorkflow) Execute(ctx workflow.Context, input []byte) (result []byte, err workflow.Error) {

	// Get Greeting.
	greetActivityParams := serializeParams("getGreetingActivity", nil)
	greetResult, err := ctx.ExecuteActivity(greetActivityParams)
	if err != nil {
		return nil, err
	}

	// Get Name.
	nameActivityParams := serializeParams("getNameActivity", nil)
	nameResult, err := ctx.ExecuteActivity(nameActivityParams)
	if err != nil {
		return nil, err
	}

	// Say Greeting.
	request := &sayGreetingActivityRequest{Name: string(nameResult), Greeting: string(greetResult)}
	sayGreetInput, err1 := json.Marshal(request)
	if err != nil {
		return nil, &workflowError{reason: err1.Error()}
	}

	sayGreetingActivityParams := serializeParams("sayGreetingActivity", sayGreetInput)
	_, err = ctx.ExecuteActivity(sayGreetingActivityParams)
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
