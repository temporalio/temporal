package taskpoller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/testvars"
)

type (
	Helper interface {
		Helper()
	}
	TaskPoller struct {
		t         Helper
		client    workflowservice.WorkflowServiceClient
		namespace string
	}
	workflowTaskPoller struct {
		*TaskPoller
		pollWorkflowTaskRequest *workflowservice.PollWorkflowTaskQueueRequest
	}
	activityTaskPoller struct {
		*TaskPoller
		pollActivityTaskRequest *workflowservice.PollActivityTaskQueueRequest
	}
	nexusTaskPoller struct {
		*TaskPoller
		pollNexusTaskRequest *workflowservice.PollNexusTaskQueueRequest
	}
	options struct {
		tv      *testvars.TestVars
		timeout time.Duration
		ctx     context.Context
	}
	optionFunc func(*options)
)

var (
	// DrainWorkflowTask returns an empty RespondWorkflowTaskCompletedRequest
	DrainWorkflowTask = func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}
	// CompleteActivityTask returns a RespondActivityTaskCompletedRequest with an auto-generated `Result` from `tv.Any().Payloads()`.
	CompleteActivityTask = func(tv *testvars.TestVars) func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			return &workflowservice.RespondActivityTaskCompletedRequest{
				Result: tv.Any().Payloads(),
			}, nil
		}
	}
	// WithTimeout defines a timeout for a task poller method (includes *all* RPC calls it has to make)
	WithTimeout = func(timeout time.Duration) optionFunc {
		return func(o *options) {
			o.timeout = timeout
		}
	}
	// WithTimeout sets a context for a task poller method (includes *all* RPC calls it has to make)
	WithContext = func(ctx context.Context) optionFunc {
		return func(o *options) {
			o.ctx = ctx
		}
	}
	NoWorkflowTaskAvailable = errors.New("taskpoller test helper timed out while waiting for the PollWorkflowTaskQueue API response, meaning no workflow task was ever created")
	NoActivityTaskAvailable = errors.New("taskpoller test helper timed out while waiting for the PollActivityTaskQueue API response, meaning no activity task was ever created")
)

func New(
	t Helper,
	client workflowservice.WorkflowServiceClient,
	namespace string,
) *TaskPoller {
	return &TaskPoller{
		t:         t,
		client:    client,
		namespace: namespace,
	}
}

// PollWorkflowTask creates a workflow task poller that uses the given PollWorkflowTaskQueueRequest.
func (p *TaskPoller) PollWorkflowTask(
	req *workflowservice.PollWorkflowTaskQueueRequest,
) *workflowTaskPoller {
	return &workflowTaskPoller{TaskPoller: p, pollWorkflowTaskRequest: req}
}

func (p *TaskPoller) PollNexusTask(
	req *workflowservice.PollNexusTaskQueueRequest,
) *nexusTaskPoller {
	return &nexusTaskPoller{TaskPoller: p, pollNexusTaskRequest: req}
}

// PollAndHandleWorkflowTask issues a PollWorkflowTaskQueueRequest to obtain a new workflow task,
// invokes the handler with the task, and completes/fails the task accordingly. Is it a blocking call.
// Any unspecified but required request and response fields are automatically generated using `tv`.
// Returning an error from `handler` fails the task.
// If no task is available, it returns `NoWorkflowTaskAvailable`.
func (p *TaskPoller) PollAndHandleWorkflowTask(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
	opts ...optionFunc,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	return p.
		PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{}).
		HandleTask(tv, handler, opts...)
}

// HandleTask invokes the provided handler with the task poll result, and completes/fails the task accordingly.
// Is it a blocking call.
// Any unspecified but required request and response fields are automatically generated using `tv`.
// Returning an error from `handler` fails the task.
// If no task is available, it returns `NoWorkflowTaskAvailable`.
func (p *workflowTaskPoller) HandleTask(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
	opts ...optionFunc,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	options := newOptions(tv, opts)
	ctx, cancel := newContext(options)
	defer cancel()
	return p.pollAndHandleTask(ctx, options, handler)
}

func (p *nexusTaskPoller) pollTask(
	ctx context.Context,
	opts *options,
) (*workflowservice.PollNexusTaskQueueResponse, error) {
	p.t.Helper()

	req := common.CloneProto(p.pollNexusTaskRequest)
	if req.Namespace == "" {
		req.Namespace = p.namespace
	}
	if req.TaskQueue == nil {
		req.TaskQueue = opts.tv.TaskQueue()
	}
	if req.Identity == "" {
		req.Identity = opts.tv.WorkerIdentity()
	}
	resp, err := p.client.PollNexusTaskQueue(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.TaskToken == nil {
		return nil, NoWorkflowTaskAvailable
	}

	return resp, err
}

func (p *nexusTaskPoller) pollAndHandleTask(
	ctx context.Context,
	opts *options,
	handler func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error),
) (*workflowservice.RespondNexusTaskCompletedResponse, error) {
	p.t.Helper()
	task, err := p.pollTask(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to poll nexus task: %w", err)
	}
	return p.handleTask(ctx, opts, task, handler)
}
func (p *nexusTaskPoller) handleTask(
	ctx context.Context,
	opts *options,
	task *workflowservice.PollNexusTaskQueueResponse,
	handler func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error),
) (*workflowservice.RespondNexusTaskCompletedResponse, error) {
	p.t.Helper()
	reply, err := handler(task)
	if err != nil {
		return nil, p.respondNexusTaskFailed(ctx, opts, task.TaskToken)
	}

	resp, err := p.respondNexusTaskCompleted(ctx, opts, task, reply)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p *nexusTaskPoller) respondNexusTaskCompleted(
	ctx context.Context,
	opts *options,
	task *workflowservice.PollNexusTaskQueueResponse,
	reply *workflowservice.RespondNexusTaskCompletedRequest,
) (*workflowservice.RespondNexusTaskCompletedResponse, error) {
	p.t.Helper()
	if reply == nil {
		return nil, errors.New("missing RespondWorkflowTaskCompletedRequest return")
	}
	if reply.Namespace == "" {
		reply.Namespace = p.namespace
	}
	if len(reply.TaskToken) == 0 {
		reply.TaskToken = task.TaskToken
	}
	if reply.Identity == "" {
		reply.Identity = opts.tv.WorkerIdentity()
	}
	reply.Response = &nexuspb.Response{}

	return p.client.RespondNexusTaskCompleted(ctx, reply)
}

func (p *nexusTaskPoller) respondNexusTaskFailed(
	ctx context.Context,
	opts *options,
	taskToken []byte,
) error {
	p.t.Helper()
	_, err := p.client.RespondNexusTaskFailed(
		ctx,
		&workflowservice.RespondNexusTaskFailedRequest{
			Namespace: p.namespace,
			TaskToken: taskToken,
			Identity:  opts.tv.WorkerIdentity(),
			Error: &nexuspb.HandlerError{
				ErrorType: string(nexus.HandlerErrorTypeInternal),
			},
		})
	return err
}

func (p *nexusTaskPoller) HandleTask(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error),
	opts ...optionFunc,
) (*workflowservice.RespondNexusTaskCompletedResponse, error) {
	p.t.Helper()
	options := newOptions(tv, opts)
	ctx, cancel := newContext(options)
	defer cancel()
	return p.pollAndHandleTask(ctx, options, handler)
}

// HandleWorkflowTask invokes the provided handler with the provided task, and completes/fails the task accordingly.
// Is it a blocking call.
// Any unspecified but required request and response fields are automatically generated using `tv`.
// Returning an error from `handler` fails the task.
func (p *TaskPoller) HandleWorkflowTask(
	tv *testvars.TestVars,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
	opts ...optionFunc,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	options := newOptions(tv, opts)
	ctx, cancel := newContext(options)
	defer cancel()
	wp := workflowTaskPoller{TaskPoller: p}
	return wp.handleTask(ctx, options, task, handler)
}

// PollActivityTask creates an activity task poller that uses the given PollActivityTaskQueueRequest.
func (p *TaskPoller) PollActivityTask(
	req *workflowservice.PollActivityTaskQueueRequest,
) *activityTaskPoller {
	return &activityTaskPoller{TaskPoller: p, pollActivityTaskRequest: req}
}

// PollAndHandleActivityTask issues a PollActivityTaskQueueRequest to obtain a new activity task,
// invokes the handler with the task, and completes/fails the task accordingly. Is it a blocking call.
// Any unspecified but required request and response fields are automatically generated using `tv`.
// Returning an error from `handler` fails the task.
// If no task is available, it returns `NoActivityTaskAvailable`.
func (p *TaskPoller) PollAndHandleActivityTask(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
	opts ...optionFunc,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	return p.
		PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{}).
		HandleTask(tv, handler, opts...)
}

// HandleActivityTask invokes the provided handler with the provided task, and completes/fails the task accordingly.
// Is it a blocking call.
// Any unspecified but required request and response fields are automatically generated using `tv`.
// Returning an error from `handler` fails the task.
func (p *TaskPoller) HandleActivityTask(
	tv *testvars.TestVars,
	task *workflowservice.PollActivityTaskQueueResponse,
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
	opts ...optionFunc,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	p.t.Helper()
	options := newOptions(tv, opts)
	ctx, cancel := newContext(options)
	defer cancel()
	ap := activityTaskPoller{TaskPoller: p}
	return ap.handleTask(ctx, options, task, handler)
}

// HandleTask invokes the provided handler with the task poll result, and completes/fails the task accordingly.
// Is it a blocking call.
// Any unspecified but required request and response fields are automatically generated using `tv`.
// Returning an error from `handler` fails the task.
// If no task is available, it returns `NoActivityTaskAvailable`.
func (p *activityTaskPoller) HandleTask(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
	opts ...optionFunc,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	p.t.Helper()
	options := newOptions(tv, opts)
	ctx, cancel := newContext(options)
	defer cancel()
	return p.pollAndHandleTask(ctx, options, handler)
}

//revive:disable-next-line:cognitive-complexity
func (p *workflowTaskPoller) pollTask(
	ctx context.Context,
	opts *options,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	p.t.Helper()

	req := common.CloneProto(p.pollWorkflowTaskRequest)
	if req.Namespace == "" {
		req.Namespace = p.namespace
	}
	if req.TaskQueue == nil {
		req.TaskQueue = opts.tv.TaskQueue()
	}
	if req.Identity == "" {
		req.Identity = opts.tv.WorkerIdentity()
	}
	resp, err := p.client.PollWorkflowTaskQueue(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.TaskToken == nil {
		return nil, NoWorkflowTaskAvailable
	}

	var events []*historypb.HistoryEvent
	history := resp.History
	if history == nil {
		return nil, errors.New("history is nil")
	}

	events = history.Events
	if len(events) == 0 && req.TaskQueue.GetKind() != enumspb.TASK_QUEUE_KIND_STICKY {
		return nil, errors.New("history events are empty")
	}

	nextPageToken := resp.NextPageToken
	for nextPageToken != nil {
		resp, err := p.client.GetWorkflowExecutionHistory(
			ctx,
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:     p.namespace,
				Execution:     resp.WorkflowExecution,
				NextPageToken: nextPageToken,
			})
		if err != nil {
			return nil, err
		}
		events = append(events, resp.History.Events...)
		nextPageToken = resp.NextPageToken
	}

	return resp, err
}

func (p *workflowTaskPoller) HandleLegacyQuery(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error),
	opts ...optionFunc,
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	p.t.Helper()
	options := newOptions(tv, opts)
	ctx, cancel := newContext(options)
	defer cancel()
	return p.pollAndHandleLegacyQuery(ctx, options, handler)
}

func (p *workflowTaskPoller) pollAndHandleLegacyQuery(
	ctx context.Context,
	opts *options,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error),
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	p.t.Helper()
	task, err := p.pollTask(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to poll workflow task: %w", err)
	}
	return p.handleQuery(ctx, task, handler)
}

func (p *workflowTaskPoller) handleQuery(
	ctx context.Context,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error),
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	p.t.Helper()
	// if an error is received here it shall be present in RespondQueryTaskCompletedResponse.ErrorMessage
	reply, err := handler(task)
	resp, err := p.respondQueryTaskCompleted(ctx, task, reply, err)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p *workflowTaskPoller) pollAndHandleTask(
	ctx context.Context,
	opts *options,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	task, err := p.pollTask(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to poll workflow task: %w", err)
	}
	return p.handleTask(ctx, opts, task, handler)
}

func (p *workflowTaskPoller) handleTask(
	ctx context.Context,
	opts *options,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	reply, err := handler(task)
	if err != nil {
		return nil, p.respondTaskFailed(ctx, opts, task.TaskToken, err)
	}

	resp, err := p.respondTaskCompleted(ctx, opts, task, reply)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p *workflowTaskPoller) respondQueryTaskCompleted(
	ctx context.Context,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	reply *workflowservice.RespondQueryTaskCompletedRequest,
	err error,
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	p.t.Helper()
	if task == nil {
		return nil, errors.New("missing PollWorkflowTaskQueueResponse")
	}
	if task.Query == nil {
		return nil, errors.New("missing Legacy Query in PollWorkflowTaskQueueResponse")
	}
	if reply == nil {
		return nil, errors.New("missing RespondQueryTaskCompletedRequest")
	}

	// setting the fields for RespondQueryTaskCompletedResponse
	if reply.Namespace == "" {
		reply.Namespace = p.namespace
	}
	if reply.TaskToken == nil {
		reply.TaskToken = task.TaskToken
	}

	if err != nil {
		reply.ErrorMessage = err.Error()
		reply.Failure = &failurepb.Failure{
			Message: err.Error(),
		}
		reply.CompletedType = enumspb.QUERY_RESULT_TYPE_FAILED
	} else {
		reply.CompletedType = enumspb.QUERY_RESULT_TYPE_ANSWERED
		if reply.QueryResult == nil {
			reply.QueryResult = payloads.EncodeString("query-result")
		}
	}

	resp, err := p.client.RespondQueryTaskCompleted(ctx, reply)
	if err != nil {
		return nil, fmt.Errorf("failed to respond with respondQueryTaskCompleted: %w", err)
	}
	return resp, nil
}

func (p *workflowTaskPoller) respondTaskCompleted(
	ctx context.Context,
	opts *options,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	reply *workflowservice.RespondWorkflowTaskCompletedRequest,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	if reply == nil {
		return nil, errors.New("missing RespondWorkflowTaskCompletedRequest return")
	}
	if reply.Namespace == "" {
		reply.Namespace = p.namespace
	}
	if len(reply.TaskToken) == 0 {
		reply.TaskToken = task.TaskToken
	}
	if reply.Identity == "" {
		reply.Identity = opts.tv.WorkerIdentity()
	}

	return p.client.RespondWorkflowTaskCompleted(ctx, reply)
}

func (p *workflowTaskPoller) respondTaskFailed(
	ctx context.Context,
	opts *options,
	taskToken []byte,
	taskErr error,
) error {
	p.t.Helper()
	_, err := p.client.RespondWorkflowTaskFailed(
		ctx,
		&workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: p.namespace,
			TaskToken: taskToken,
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
			Failure:   temporal.GetDefaultFailureConverter().ErrorToFailure(taskErr),
			Identity:  opts.tv.WorkerIdentity(),
		})
	return err
}

func (p *activityTaskPoller) pollActivityTask(
	ctx context.Context,
	opts *options,
) (*workflowservice.PollActivityTaskQueueResponse, error) {
	p.t.Helper()

	req := common.CloneProto(p.pollActivityTaskRequest)
	if req.Namespace == "" {
		req.Namespace = p.namespace
	}
	if req.TaskQueue == nil {
		req.TaskQueue = opts.tv.TaskQueue()
	}
	if req.Identity == "" {
		req.Identity = opts.tv.WorkerIdentity()
	}
	resp, err := p.client.PollActivityTaskQueue(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.TaskToken) == 0 {
		return nil, NoActivityTaskAvailable
	}

	return resp, err
}

func (p *activityTaskPoller) pollAndHandleTask(
	ctx context.Context,
	opts *options,
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	p.t.Helper()
	task, err := p.pollActivityTask(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to poll activity task: %w", err)
	}
	return p.handleTask(ctx, opts, task, handler)
}

// TODO: support cancelling activity task
func (p *activityTaskPoller) handleTask(
	ctx context.Context,
	opts *options,
	task *workflowservice.PollActivityTaskQueueResponse,
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	p.t.Helper()
	reply, err := handler(task)
	if err != nil {
		return nil, p.respondTaskFailed(ctx, opts, task, err)
	}

	resp, err := p.respondTaskCompleted(ctx, opts, task, reply)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p *activityTaskPoller) respondTaskCompleted(
	ctx context.Context,
	opts *options,
	task *workflowservice.PollActivityTaskQueueResponse,
	reply *workflowservice.RespondActivityTaskCompletedRequest,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	p.t.Helper()
	if reply == nil {
		return nil, errors.New("missing RespondActivityTaskCompletedRequest return")
	}
	if reply.Namespace == "" {
		reply.Namespace = p.namespace
	}
	if len(reply.TaskToken) == 0 {
		reply.TaskToken = task.TaskToken
	}
	if reply.Identity == "" {
		reply.Identity = opts.tv.WorkerIdentity()
	}

	return p.client.RespondActivityTaskCompleted(ctx, reply)
}

func (p *activityTaskPoller) respondTaskFailed(
	ctx context.Context,
	opts *options,
	task *workflowservice.PollActivityTaskQueueResponse,
	taskErr error,
) error {
	p.t.Helper()
	_, err := p.client.RespondActivityTaskFailed(
		ctx,
		&workflowservice.RespondActivityTaskFailedRequest{
			Namespace: p.namespace,
			TaskToken: task.TaskToken,
			Failure:   temporal.GetDefaultFailureConverter().ErrorToFailure(taskErr),
			Identity:  opts.tv.WorkerIdentity(),
		})
	return err
}

func newOptions(
	tv *testvars.TestVars,
	opts []optionFunc,
) *options {
	res := &options{tv: tv}

	// default options
	WithTimeout(21 * time.Second)(res) // Server logs warning if long poll is less than 20s

	// custom options
	for _, f := range opts {
		f(res)
	}

	return res
}

func newContext(opts *options) (context.Context, context.CancelFunc) {
	if opts.ctx != nil {
		return rpc.NewContextFromParentWithTimeoutAndVersionHeaders(opts.ctx, opts.timeout*debug.TimeoutMultiplier)
	}
	return rpc.NewContextWithTimeoutAndVersionHeaders(opts.timeout * debug.TimeoutMultiplier)
}
