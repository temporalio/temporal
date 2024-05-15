package playground

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

const (
	taskQueue = "my-task-queue"
)

func starter(c client.Client) chan bool {
	done := make(chan bool)

	go func() {
		fmt.Println("[[ STARTER START ]]")
		workflowOptions := client.StartWorkflowOptions{
			ID:               WORKFLOW_ID,
			TaskQueue:        taskQueue,
			EnableEagerStart: true,
		}
		we, err := c.ExecuteWorkflow(context.Background(), workflowOptions, Workflow)
		if err != nil {
			panic(err)
		}

		handle, err := c.UpdateWorkflow(context.Background(), we.GetID(), we.GetRunID(), "UPDATE", 5)
		if err != nil {
			fmt.Println("Update error", err)
		}
		var updateResult int
		err = handle.Get(context.Background(), &updateResult)
		if err != nil {
			fmt.Println("Update error", err)
		}

		if err = c.SignalWorkflow(context.Background(), we.GetID(), we.GetRunID(), "SIGNAL", nil); err != nil {
			fmt.Println("Signal error", err)
		}

		var res int
		if err = we.Get(context.Background(), &res); err != nil {
			fmt.Println("Workflow error", err)
		}

		fmt.Println("Workflow Result: ", res)
		fmt.Println("[[ STARTER END ]]")

		done <- true
	}()

	return done
}

func Workflow(ctx workflow.Context) (int, error) {
	fmt.Println("[[ WORKFLOW START ]]")

	counter := 0

	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		"UPDATE",
		func(ctx workflow.Context, i int) (int, error) {
			tmp := counter
			counter += i
			return tmp, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, i int) error {
			if i < 0 {
				return fmt.Errorf("addend must be non-negative (%v)", i)
			}
			return nil
		}},
	); err != nil {
		return 0, err
	}

	_ = workflow.GetSignalChannel(ctx, "SIGNAL").Receive(ctx, nil)

	fmt.Println("[[ WORKFLOW END ]]")
	return counter, ctx.Err()
}
