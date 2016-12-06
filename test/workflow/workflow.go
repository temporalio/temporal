package workflow

import (
	"code.uber.internal/devexp/minions/common/coroutine"
	"code.uber.internal/devexp/minions/test/flow"
)

// Error to return from Workflow and Activity implementations.
type Error interface {
	error
	Reason() string
	Details() []byte
}

// ActivityClient is used to invoke activities from a workflow definition
type ActivityClient interface {
	ExecuteActivity(parameters flow.ExecuteActivityParameters) (result []byte, err Error)
}

// Func is a function used to spawn workflow execution through Context.Go
type Func func(ctx Context)

// Context of a workflow execution
type Context interface {
	coroutine.Context
	ActivityClient
	WorkflowInfo() *flow.WorkflowInfo
	Go(f Func) // Must be used to create goroutines inside a workflow code
}

// Workflow is an interface that any workflow should implement.
// Code of a workflow must use coroutine.Channel, coroutine.Selector, and Context.Go instead of
// native channels, select and go.
type Workflow interface {
	Execute(ctx Context, input []byte) (result []byte, err Error)
}

// NewWorkflowDefinition creates a flow.WorkflowDefinition from a Workflow
func NewWorkflowDefinition(workflow Workflow) flow.WorkflowDefinition {
	return &workflowDefinition{workflow: workflow}
}

type workflowDefinition struct {
	workflow Workflow
}

type workflowResult struct {
	workflowResult []byte
	error          Error
}

type contextImpl struct {
	coroutine.Context
	wc         flow.WorkflowContext
	dispatcher coroutine.Dispatcher
	result     **workflowResult
}

type activityClient struct {
	dispatcher  coroutine.Dispatcher
	asyncClient flow.AsyncActivityClient
}

func (d *workflowDefinition) Execute(wc flow.WorkflowContext, input []byte) {
	var resultPtr *workflowResult
	c := &contextImpl{
		wc:     wc,
		result: &resultPtr,
	}
	c.dispatcher = coroutine.NewDispatcher(func(ctx coroutine.Context) {
		c.Context = ctx
		r := &workflowResult{}
		r.workflowResult, r.error = d.workflow.Execute(c, input)
		*c.result = r
	})
	c.executeDispatcher()
}

// executeDispatcher executed coroutines in the calling thread and calls workflow completion callbacks
// if root workflow function returned
func (c *contextImpl) executeDispatcher() {
	c.dispatcher.ExecuteUntilAllBlocked()
	r := *c.result
	if r == nil {
		return
	}
	// Cannot cast nil values from interface to interface
	var err flow.Error
	if r.error != nil {
		err = r.error.(flow.Error)
	}
	c.wc.Complete(r.workflowResult, err)
	c.dispatcher.Close()
}

func (c *contextImpl) ExecuteActivity(parameters flow.ExecuteActivityParameters) (result []byte, err Error) {
	resultChannel := c.NewBufferedChannel(1)
	c.wc.ExecuteActivity(parameters, func(r []byte, e flow.Error) {
		result = r
		if e != nil {
			err = e.(Error)
		}
		ok := resultChannel.SendAsync(true)
		if !ok {
			panic("unexpected")
		}
		c.executeDispatcher()
	})
	_, _ = resultChannel.Recv(c)
	return
}

func (c *contextImpl) Go(f Func) {
	c.NewCoroutine(func(ctx coroutine.Context) {
		context := &contextImpl{
			Context:    ctx,
			wc:         c.wc,
			dispatcher: c.dispatcher,
			result:     c.result,
		}
		f(context)
	})
}

func (c *contextImpl) WorkflowInfo() *flow.WorkflowInfo {
	return c.wc.WorkflowInfo()
}

// GetContext from flow.ContextProvider interface
func (c *contextImpl) GetContext() coroutine.Context {
	return c.Context
}
