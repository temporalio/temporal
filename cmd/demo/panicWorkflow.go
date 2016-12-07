package main

import (
	"code.uber.internal/devexp/minions/test/flow"
	"code.uber.internal/devexp/minions/test/workflow"
)

type (
	panicWorkflow struct {
	}

	simpleActivity struct {
	}
)

func (w panicWorkflow) Execute(ctx workflow.Context, input []byte) (result []byte, err workflow.Error) {

	simpleActivityParams := serializeParams("simpleActivity", nil)
	_, err = ctx.ExecuteActivity(simpleActivityParams)
	if err != nil {
		panic("Simulated failure")
	}
	return nil, nil
}

func (g simpleActivity) Execute(context flow.ActivityExecutionContext, input []byte) ([]byte, flow.Error) {
	return nil, &workflowError{reason: "failed connection"}
}
