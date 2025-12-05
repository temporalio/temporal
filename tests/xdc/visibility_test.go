package xdc

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type VisibilityTestSuite struct {
	xdcBaseSuite
}

func TestVisibilityTestSuite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		enableTransitionHistory bool
	}{
		{
			name:                    "EnableTransitionHistory",
			enableTransitionHistory: true,
		},
		{
			name:                    "DisableTransitionHistory",
			enableTransitionHistory: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &VisibilityTestSuite{}
			s.enableTransitionHistory = tc.enableTransitionHistory
			suite.Run(t, s)
		})
	}
}

func (s *VisibilityTestSuite) SetupSuite() {
	s.setupSuite()
}

func (s *VisibilityTestSuite) SetupTest() {
	s.setupTest()
}

func (s *VisibilityTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *VisibilityTestSuite) TestSearchAttributes() {
	ns := s.createGlobalNamespace()
	if testcore.UseSQLVisibility() {
		// When Elasticsearch is enabled, the search attribute aliases are not used.
		updateNamespaceConfig(s.Assertions, ns,
			func() *namespacepb.NamespaceConfig {
				return &namespacepb.NamespaceConfig{
					CustomSearchAttributeAliases: map[string]string{
						"Bool01":     "CustomBoolField",
						"Datetime01": "CustomDatetimeField",
						"Double01":   "CustomDoubleField",
						"Int01":      "CustomIntField",
						"Keyword01":  "CustomKeywordField",
						"Text01":     "CustomTextField",
					},
				}
			},
			s.clusters, 0)
	}

	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "xdc-search-attr-test-" + uuid.NewString()
	wt := "xdc-search-attr-test-type"
	tl := "xdc-search-attr-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomTextField": payload.EncodeString("test value"),
		},
	}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		SearchAttributes:    searchAttr,
	}
	startTime := time.Now().UTC()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution \n", tag.WorkflowRunID(we.GetRunId()))

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = timestamppb.New(startTime)
	saListRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: ns,
		PageSize:  5,
		Query:     fmt.Sprintf(`WorkflowId = "%s" and CustomTextField = "test value"`, id),
	}

	testListResult := func(client workflowservice.WorkflowServiceClient, lr *workflowservice.ListWorkflowExecutionsRequest) {
		var openExecution *workflowpb.WorkflowExecutionInfo

		s.Eventually(func() bool {
			startFilter.LatestTime = timestamppb.New(time.Now().UTC())

			resp, err := client.ListWorkflowExecutions(testcore.NewContext(), lr)
			s.NoError(err)
			if len(resp.GetExecutions()) == 1 {
				openExecution = resp.GetExecutions()[0]
				return true
			}
			return false
		}, 20*time.Second, 100*time.Millisecond)
		s.NotNil(openExecution)
		s.Equal(we.GetRunId(), openExecution.GetExecution().GetRunId())
		searchValPayload := openExecution.GetSearchAttributes().GetIndexedFields()["CustomTextField"]
		var searchVal string
		err = payload.Decode(searchValPayload, &searchVal)
		s.NoError(err)
		s.Equal("test value", searchVal)
	}

	// List workflow in active
	engine1 := s.clusters[0].FrontendClient()
	testListResult(engine1, saListRequest)

	// List workflow in standby
	engine2 := s.clusters[1].FrontendClient()
	testListResult(engine2, saListRequest)

	// upsert search attributes
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		upsertCommand := &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
			Attributes: &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{UpsertWorkflowSearchAttributesCommandAttributes: &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
				SearchAttributes: getUpsertSearchAttributes(),
			}}}

		return []*commandpb.Command{upsertCommand}, nil
	}

	// nolint
	poller0 := testcore.TaskPoller{
		Client:              client0,
		Namespace:           ns,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller0.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	testListResult = func(client workflowservice.WorkflowServiceClient, lr *workflowservice.ListWorkflowExecutionsRequest) {
		s.Eventually(func() bool {
			resp, err := client.ListWorkflowExecutions(testcore.NewContext(), lr)
			s.NoError(err)
			if len(resp.GetExecutions()) != 1 {
				return false
			}
			fields := resp.GetExecutions()[0].SearchAttributes.GetIndexedFields()
			if len(fields) != 3 {
				return false
			}

			searchValBytes := fields["CustomTextField"]
			var searchVal string
			payload.Decode(searchValBytes, &searchVal)
			s.Equal("another string", searchVal)

			searchValBytes2 := fields["CustomIntField"]
			var searchVal2 int
			payload.Decode(searchValBytes2, &searchVal2)
			s.Equal(123, searchVal2)

			buildIdsBytes := fields[sadefs.BuildIds]
			var buildIds []string
			err = payload.Decode(buildIdsBytes, &buildIds)
			s.NoError(err)
			s.Equal([]string{worker_versioning.UnversionedSearchAttribute}, buildIds)

			return true
		}, 20*time.Second, 100*time.Millisecond)
	}

	saListRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: ns,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowId = "%s" and CustomTextField = "another string"`, id),
	}

	// test upsert result in active
	testListResult(engine1, saListRequest)
	// test upsert result in standby
	testListResult(engine2, saListRequest)

	runningListRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: ns,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	// test upsert result in active
	testListResult(engine1, runningListRequest)
	// test upsert result in standby
	testListResult(engine2, runningListRequest)

	// terminate workflow
	terminateReason := "force terminate to make sure standby process tasks"
	terminateDetails := payloads.EncodeString("terminate details")
	_, err = client0.TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   terminateReason,
		Details:  terminateDetails,
		Identity: identity,
	})
	s.NoError(err)

	// check terminate done
	getHistoryReq := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}

	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 UpsertWorkflowSearchAttributes
  6 v1 WorkflowExecutionTerminated {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"force terminate to make sure standby process tasks"}`,
		func() *historypb.History {
			historyResponse, err := client0.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			s.NoError(err)
			return historyResponse.History
		}, 1*time.Second, 100*time.Millisecond)

	// check history replicated to the other cluster
	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 UpsertWorkflowSearchAttributes
  6 v1 WorkflowExecutionTerminated {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"force terminate to make sure standby process tasks"}`,
		func() *historypb.History {
			historyResponse, err := client1.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			if err != nil {
				return nil
			}
			return historyResponse.History
		}, 20*time.Second, 100*time.Millisecond)

	terminatedListRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: ns,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Terminated'`, wt),
	}
	// test upsert result in active
	testListResult(engine1, terminatedListRequest)
	// test upsert result in standby
	testListResult(engine2, terminatedListRequest)
}

// TODO (alex): remove this func.
func getUpsertSearchAttributes() *commonpb.SearchAttributes {
	attrValPayload2, _ := payload.Encode(123)
	upsertSearchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomTextField": payload.EncodeString("another string"),
			"CustomIntField":  attrValPayload2,
		},
	}
	return upsertSearchAttr
}
