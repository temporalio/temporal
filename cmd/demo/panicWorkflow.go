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

// panicWorkflow decider code.
func (w panicWorkflow) Execute(ctx workflow.Context, input []byte) (result []byte, err workflow.Error) {

	c1 := ctx.NewChannel()

	ctx.Go(func(ctx workflow.Context) {
		simpleActivityParams := serializeParams("simpleActivity", nil)
		_, err = ctx.ExecuteActivity(simpleActivityParams)

		if err != nil {
			panic("Simulated failure")
		}
		c1.Send(ctx, true)
	})

	c1.Recv(ctx)

	return nil, nil
}

// simpleActivity activity that fails with error.
func (g simpleActivity) Execute(context flow.ActivityExecutionContext, input []byte) ([]byte, flow.Error) {
	return nil, &workflowError{reason: "failed connection"}
}
