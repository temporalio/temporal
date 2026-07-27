package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// WorkflowCompletionPaginationTestSuite exercises workflow task completion
// pagination end to end, driving multi-page RespondWorkflowTaskCompleted requests
// directly against the frontend
type WorkflowCompletionPaginationTestSuite struct {
	parallelsuite.Suite[*WorkflowCompletionPaginationTestSuite]
}

func TestWorkflowCompletionPaginationTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowCompletionPaginationTestSuite{})
}

const (
	// markerPayloadSize is each RecordMarker command's payload size
	markerPayloadSize = 512 * 1024
	// maxPageBytes bounds how many markers ride on one page so each request stays
	// under the gRPC max message size.
	maxPageBytes = 1536 * 1024
)

// newPaginationEnv builds a dedicated test cluster with pagination enabled. The
// merged completions stay under the default per-event blob, history size, and
// history count error limits, so those need no overrides. We only force a small
// per-batch size so a merged completion spans multiple history batches, and raise
// the pagination buffer limit enough to hold the largest test completion.
func (s *WorkflowCompletionPaginationTestSuite) newPaginationEnv() *testcore.TestEnv {
	return testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.MaximumEventBatchSizeInBytes, 1024*1024),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowTaskCompletionPagination, true),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTaskCompletionBufferTotalSizeLimit, 1024*1024*1024),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTaskCompletionBufferSizeLimit, 256*1024*1024),
	)
}

// startWorkflowAndStartWFT starts a workflow and polls (starts) its first
// workflow task, returning the execution, its task queue, and the started task token.
func (s *WorkflowCompletionPaginationTestSuite) startWorkflowAndStartWFT(
	env *testcore.TestEnv,
	wfTaskTimeout time.Duration,
) (*commonpb.WorkflowExecution, *taskqueuepb.TaskQueue, []byte) {
	id := uuid.NewString()
	taskQueue := &taskqueuepb.TaskQueue{Name: id, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	startResp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "functional-workflow-completion-pagination"},
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(5 * time.Minute),
		WorkflowTaskTimeout: durationpb.New(wfTaskTimeout),
		Identity:            "worker1",
	})
	s.NoError(err)

	we := &commonpb.WorkflowExecution{WorkflowId: id, RunId: startResp.RunId}

	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  "worker1",
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken)

	return we, taskQueue, pollResp.TaskToken
}

// makeMarkerCommands builds n RecordMarker commands, each carrying a payload of
// markerPayloadSize, so the merged completion reaches a predictable size.
func makeMarkerCommands(n int) []*commandpb.Command {
	cmds := make([]*commandpb.Command, n)
	payload := payloads.EncodeBytes(make([]byte, markerPayloadSize))
	for i := range cmds {
		cmds[i] = &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
			Attributes: &commandpb.Command_RecordMarkerCommandAttributes{
				RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: fmt.Sprintf("marker-%d", i),
					Details:    map[string]*commonpb.Payloads{"data": payload},
				},
			},
		}
	}
	return cmds
}

// chunkCommandsIntoPages splits commands into pages each holding at most
// markersPerPage commands, so individual page requests stay under the gRPC size
// limit.
func chunkCommandsIntoPages(cmds []*commandpb.Command) [][]*commandpb.Command {
	markersPerPage := max(maxPageBytes/markerPayloadSize, 1)
	var pages [][]*commandpb.Command
	for start := 0; start < len(cmds); start += markersPerPage {
		end := min(start+markersPerPage, len(cmds))
		pages = append(pages, cmds[start:end])
	}
	return pages
}

// sendIntermediatePages sends pages 0..len(pages)-1 as intermediate pages and
// returns the next page number to use for the final page. It asserts each
// intermediate page is acknowledged with an empty OK.
func (s *WorkflowCompletionPaginationTestSuite) sendIntermediatePages(
	env *testcore.TestEnv,
	taskToken []byte,
	pages [][]*commandpb.Command,
) int32 {
	for i, page := range pages {
		resp, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:        env.Namespace().String(),
			TaskToken:        taskToken,
			Commands:         page,
			IntermediatePage: true,
			PageNumber:       int32(i),
			Identity:         "worker1",
		})
		s.NoError(err)
		s.NotNil(resp)
	}
	return int32(len(pages))
}

// TestLargeMergedCompletion sends a ~12 MB RespondWorkflowTaskCompleted request
// split into several pages
func (s *WorkflowCompletionPaginationTestSuite) TestLargeMergedCompletion() {
	env := s.newPaginationEnv()

	const totalMarkers = 24
	we, _, taskToken := s.startWorkflowAndStartWFT(env, time.Minute)

	// All but the final marker travel on intermediate pages; the final page
	// carries the last marker plus the terminal CompleteWorkflowExecution command.
	markers := makeMarkerCommands(totalMarkers)
	intermediate := chunkCommandsIntoPages(markers[:totalMarkers-1])
	finalPageNumber := s.sendIntermediatePages(env, taskToken, intermediate)

	finalCommands := []*commandpb.Command{markers[totalMarkers-1]}
	finalCommands = append(finalCommands, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("done"),
			},
		},
	})

	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:  env.Namespace().String(),
		TaskToken:  taskToken,
		Commands:   finalCommands,
		PageNumber: finalPageNumber,
		Identity:   "worker1",
	})
	s.NoError(err)

	// All markers are recorded, exactly one WorkflowTaskCompleted event is
	// written, the workflow completes, and nothing failed.
	history := env.GetHistory(env.Namespace().String(), we)
	s.Equal(totalMarkers, countEvents(history, enumspb.EVENT_TYPE_MARKER_RECORDED))
	s.Equal(1, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED))
	s.Equal(0, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED))
	s.Equal(1, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED))
}

// TestLargeMergedContinueAsNew sends a ~16 MB RespondWorkflowTaskCompleted request
// split into several pages that ends in continue-as-new. Unlike a plain completion,
// continue-as-new closes the current run and starts a fresh one in the same
// transaction that flushes the merged buffer, so it exercises a different buffer
// clearing path.
func (s *WorkflowCompletionPaginationTestSuite) TestLargeMergedContinueAsNew() {
	env := s.newPaginationEnv()

	we, taskQueue, taskToken := s.startWorkflowAndStartWFT(env, time.Minute)

	const totalMarkers = 32
	markers := makeMarkerCommands(totalMarkers)
	intermediate := chunkCommandsIntoPages(markers)
	finalPageNumber := s.sendIntermediatePages(env, taskToken, intermediate)

	finalCommands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: "functional-workflow-completion-pagination"},
				TaskQueue:    taskQueue,
			},
		},
	}}

	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:  env.Namespace().String(),
		TaskToken:  taskToken,
		Commands:   finalCommands,
		PageNumber: finalPageNumber,
		Identity:   "worker1",
	})
	s.NoError(err)

	history := env.GetHistory(env.Namespace().String(), we)
	s.Equal(totalMarkers, countEvents(history, enumspb.EVENT_TYPE_MARKER_RECORDED))
	s.Equal(1, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW))
	s.Equal(0, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED))
}

// TestBufferLostOnShardClose checks what happens when the buffer is dropped
// between pages. Closing the shard clears the cached context and its buffered
// pages, so the final page can't find them and returns a buffer-lost error. That
// error is retryable, no WorkflowTaskFailed event is written, so the worker can
// just resend from page 0.
func (s *WorkflowCompletionPaginationTestSuite) TestBufferLostOnShardClose() {
	env := s.newPaginationEnv()

	we, _, taskToken := s.startWorkflowAndStartWFT(env, time.Minute)

	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:        env.Namespace().String(),
		TaskToken:        taskToken,
		Commands:         makeMarkerCommands(1),
		IntermediatePage: true,
		PageNumber:       0,
		Identity:         "worker1",
	})
	s.NoError(err)

	// Evict the cached context (and its buffer) by closing the shard.
	env.CloseShard(env.NamespaceID().String(), we.WorkflowId)

	// The final page can no longer find its buffered pages.
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: taskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				},
			},
		}},
		PageNumber: 1,
		Identity:   "worker1",
	})
	s.Error(err)
	var bufferLost *serviceerror.WorkflowTaskCompletionBufferLost
	s.ErrorAs(err, &bufferLost)

	history := env.GetHistory(env.Namespace().String(), we)
	s.Equal(0, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED))
	s.Equal(0, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED))
}

// TestPaginationDisabledRejected verifies that a paginated request is rejected with
// FailedPrecondition when the feature is disabled for the namespace.
func (s *WorkflowCompletionPaginationTestSuite) TestPaginationDisabledRejected() {
	env := testcore.NewEnv(s.T()) // pagination disabled by default
	_, _, taskToken := s.startWorkflowAndStartWFT(env, time.Minute)

	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:        env.Namespace().String(),
		TaskToken:        taskToken,
		Commands:         makeMarkerCommands(1),
		IntermediatePage: true,
		PageNumber:       0,
		Identity:         "worker1",
	})
	s.Error(err)
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
}

// TestBufferOverflowFailsWorkflowTask verifies that exceeding the per-workflow
// buffer limit fails the workflow task.
func (s *WorkflowCompletionPaginationTestSuite) TestBufferOverflowFailsWorkflowTask() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowTaskCompletionPagination, true),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTaskCompletionBufferSizeLimit, markerPayloadSize),
	)
	we, _, taskToken := s.startWorkflowAndStartWFT(env, time.Minute)

	// A single marker already exceeds the limit, failing the workflow task.
	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:        env.Namespace().String(),
		TaskToken:        taskToken,
		Commands:         makeMarkerCommands(1),
		IntermediatePage: true,
		PageNumber:       0,
		Identity:         "worker1",
	})
	s.Error(err)

	history := env.GetHistory(env.Namespace().String(), we)
	s.Equal(1, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED))
	s.Equal(0, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED))
}

// TestOutOfOrderPagesReassemble verifies that pages buffered out of arrival order
// are merged by page number, so the resulting commands are correctly ordered.
func (s *WorkflowCompletionPaginationTestSuite) TestOutOfOrderPagesReassemble() {
	env := s.newPaginationEnv()
	we, _, taskToken := s.startWorkflowAndStartWFT(env, time.Minute)

	markers := makeMarkerCommands(3)
	for _, p := range []int32{2, 0, 1} {
		_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:        env.Namespace().String(),
			TaskToken:        taskToken,
			Commands:         []*commandpb.Command{markers[p]},
			IntermediatePage: true,
			PageNumber:       p,
			Identity:         "worker1",
		})
		s.NoError(err)
	}

	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: taskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				},
			},
		}},
		PageNumber: 3,
		Identity:   "worker1",
	})
	s.NoError(err)

	history := env.GetHistory(env.Namespace().String(), we)
	var names []string
	for _, e := range history {
		if e.GetEventType() == enumspb.EVENT_TYPE_MARKER_RECORDED {
			names = append(names, e.GetMarkerRecordedEventAttributes().GetMarkerName())
		}
	}
	s.Equal([]string{"marker-0", "marker-1", "marker-2"}, names)
}

// TestResendAfterBufferLost verifies that after a buffer-lost error the worker can
// resend from page 0 and complete the workflow.
func (s *WorkflowCompletionPaginationTestSuite) TestResendAfterBufferLost() {
	env := s.newPaginationEnv()
	we, _, taskToken := s.startWorkflowAndStartWFT(env, time.Minute)

	bufferPage0 := func() {
		_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:        env.Namespace().String(),
			TaskToken:        taskToken,
			Commands:         makeMarkerCommands(1),
			IntermediatePage: true,
			PageNumber:       0,
			Identity:         "worker1",
		})
		s.NoError(err)
	}
	completeFinalPage := func() error {
		_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: taskToken,
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("done"),
					},
				},
			}},
			PageNumber: 1,
			Identity:   "worker1",
		})
		return err
	}

	// Buffer page 0, lose the buffer by evicting the cached context, and watch the
	// final page fail.
	bufferPage0()
	env.CloseShard(env.NamespaceID().String(), we.WorkflowId)
	var bufferLost *serviceerror.WorkflowTaskCompletionBufferLost
	s.ErrorAs(completeFinalPage(), &bufferLost)

	// Resend from page 0; the completion now succeeds.
	bufferPage0()
	s.NoError(completeFinalPage())

	history := env.GetHistory(env.Namespace().String(), we)
	s.Equal(1, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED))
	s.Equal(0, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED))
}

// TestBufferLimits drives two intermediate marker pages against different total /
// per-namespace limit combinations
func (s *WorkflowCompletionPaginationTestSuite) TestBufferLimits() {
	// process total exhausted
	s.Run("process total exhausted", func(s *WorkflowCompletionPaginationTestSuite) {
		s.runBufferLimitCase(markerPayloadSize+markerPayloadSize/2, 1.0, true)
	})
	// namespace share exhausted
	s.Run("namespace share exhausted", func(s *WorkflowCompletionPaginationTestSuite) {
		s.runBufferLimitCase(markerPayloadSize*3, 0.5, true)
	})
	// both sufficiently large
	s.Run("both limits sufficiently large", func(s *WorkflowCompletionPaginationTestSuite) {
		s.runBufferLimitCase(markerPayloadSize*100, 1.0, false)
	})
}

// runBufferLimitCase buffers page 0 (always fits), then buffers page 1. When
// expectRejected, page 1 must fail with a transient buffer-lost error and write no
// events; otherwise page 1 buffers and a final page completes the workflow.
func (s *WorkflowCompletionPaginationTestSuite) runBufferLimitCase(
	totalSizeLimit int,
	namespaceRatio float64,
	expectRejected bool,
) {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowTaskCompletionPagination, true),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTaskCompletionBufferTotalSizeLimit, totalSizeLimit),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTaskCompletionBufferNamespaceRatio, namespaceRatio),
	)
	we, _, taskToken := s.startWorkflowAndStartWFT(env, time.Minute)

	bufferMarkerPage := func(page int32) error {
		_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:        env.Namespace().String(),
			TaskToken:        taskToken,
			Commands:         makeMarkerCommands(1),
			IntermediatePage: true,
			PageNumber:       page,
			Identity:         "worker1",
		})
		return err
	}

	// Page 0 always fits.
	s.NoError(bufferMarkerPage(0))

	err := bufferMarkerPage(1)
	if expectRejected {
		// buffer-lost is transient: no WorkflowTaskFailed event, unlike the
		// per-workflow limit which fails the workflow task.
		var bufferLost *serviceerror.WorkflowTaskCompletionBufferLost
		s.ErrorAs(err, &bufferLost)

		history := env.GetHistory(env.Namespace().String(), we)
		s.Equal(0, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED))
		s.Equal(0, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED))
		return
	}

	// Both pages buffered; the final page completes the workflow.
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: taskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				},
			},
		}},
		PageNumber: 2,
		Identity:   "worker1",
	})
	s.NoError(err)

	history := env.GetHistory(env.Namespace().String(), we)
	s.Equal(2, countEvents(history, enumspb.EVENT_TYPE_MARKER_RECORDED))
	s.Equal(1, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED))
	s.Equal(0, countEvents(history, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED))
}

// countEvents returns the number of history events of the given type.
func countEvents(history []*historypb.HistoryEvent, eventType enumspb.EventType) int {
	count := 0
	for _, event := range history {
		if event.GetEventType() == eventType {
			count++
		}
	}
	return count
}
