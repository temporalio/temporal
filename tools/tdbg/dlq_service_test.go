package tdbg_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
)

type (
	dlqTestParams struct {
		dlqVersion      string
		command         string
		dlqType         string
		sourceCluster   string
		targetCluster   string
		maxMessageCount string
		lastMessageID   string
		outputFileName  string
		adminClient     *fakeAdminClient
		clientFactory   tdbg.ClientFactory
		// expectedErrSubstrings is a list of substrings that are expected to be found in the error message. We don't use an
		// exact error message to prevent this from becoming a change-detector test.
		expectedErrSubstrings []string
	}
	dlqTestCase struct {
		name           string
		version        string
		override       func(p *dlqTestParams)
		validateStdout func(t *testing.T, b *bytes.Buffer)
		validateStderr func(t *testing.T, b *bytes.Buffer)
	}
	fakeClientFactory struct {
		adminClient adminservice.AdminServiceClient
	}
	fakeAdminClient struct {
		adminservice.AdminServiceClient
		err error

		// ListQueues
		nextListQueueResponse int
		previousPageToken     []byte
		listQueueResponses    []*adminservice.ListQueuesResponse

		// GetDLQTasks (for V2)
		getDLQTasksResponses    []*adminservice.GetDLQTasksResponse
		nextGetDLQTasksResponse int
	}
)

func (tc *dlqTestCase) Run(t *testing.T) {
	faultyAdminClient := &fakeAdminClient{err: errors.New("did not expect client to be used")}
	p := dlqTestParams{
		dlqVersion:            tc.version,
		dlqType:               strconv.Itoa(tasks.CategoryTransfer.ID()),
		sourceCluster:         "test-source-cluster",
		targetCluster:         "test-target-cluster",
		maxMessageCount:       "",
		lastMessageID:         "",
		outputFileName:        "",
		expectedErrSubstrings: nil,
		adminClient:           faultyAdminClient,
		clientFactory:         fakeClientFactory{adminClient: faultyAdminClient},
	}
	tc.override(&p)
	var stdout, stderr bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = p.clientFactory
		params.Writer = &stdout
		params.ErrWriter = &stderr
	})
	runArgs := []string{
		"tdbg",
		"--" + tdbg.FlagYes,
		"dlq",
	}
	runArgs = appendArg(runArgs, tdbg.FlagDLQVersion, p.dlqVersion)
	runArgs = append(runArgs, p.command)
	if p.command != "list" {
		runArgs = appendArg(runArgs, tdbg.FlagDLQType, p.dlqType)
		runArgs = appendArg(runArgs, tdbg.FlagCluster, p.sourceCluster)
		runArgs = appendArg(runArgs, tdbg.FlagTargetCluster, p.targetCluster)
		runArgs = appendArg(runArgs, tdbg.FlagMaxMessageCount, p.maxMessageCount)
		runArgs = appendArg(runArgs, tdbg.FlagLastMessageID, p.lastMessageID)
	}
	runArgs = appendArg(runArgs, tdbg.FlagOutputFilename, p.outputFileName)

	t.Logf("Running %v", runArgs)
	err := app.Run(runArgs)
	if len(p.expectedErrSubstrings) > 0 {
		assert.Error(t, err, "Expected error to contain %v", p.expectedErrSubstrings)
		for _, s := range p.expectedErrSubstrings {
			assert.ErrorContains(t, err, s)
		}
	} else {
		assert.NoError(t, err)
	}

	if tc.validateStdout != nil {
		tc.validateStdout(t, &stdout)
	}
	if tc.validateStderr != nil {
		tc.validateStderr(t, &stderr)
	}
}

func TestDLQCommand_V2(t *testing.T) {
	for _, tc := range []dlqTestCase{
		{
			name: "read no target cluster with faulty admin client",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.targetCluster = ""
				p.adminClient.err = errors.New("some error")
				p.expectedErrSubstrings = []string{tdbg.FlagTargetCluster, "DescribeCluster", "some error"}
			},
		},
		{
			name: "read no source cluster for replication task",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.sourceCluster = ""
				p.dlqType = strconv.Itoa(tasks.CategoryReplication.ID())
				p.expectedErrSubstrings = []string{tdbg.FlagCluster, "replication", "source cluster"}
			},
		},
		{
			name: "read category is not an integer",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.dlqType = "my-string"
				p.expectedErrSubstrings = []string{"category", "integer", "my-string"}
			},
		},
		{
			name: "read invalid category",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.dlqType = "-1"
				p.expectedErrSubstrings = []string{"category", "-1"}
			},
		},
		{
			name: "zero max message count",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.maxMessageCount = "0"
				p.expectedErrSubstrings = []string{tdbg.FlagMaxMessageCount, "positive", "0"}
			},
		},
		{
			name: "last message ID is less than first possible message ID",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "last message ID is less than first possible message ID",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "invalid output file name",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.outputFileName = "\\0/"
				p.lastMessageID = "100"
				p.expectedErrSubstrings = []string{"output file", "\\0/"}
			},
		},
		{
			name: "GetDLQTasks error",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.adminClient.err = errors.New("some error")
				p.expectedErrSubstrings = []string{"some error", "GetDLQTasks"}
			},
		},
		{
			name: "GetDLQTasks on empty queue",
			override: func(p *dlqTestParams) {
				p.command = "read"
				p.dlqType = "1"
				p.adminClient.err = errors.New(" GetDLQTasks failed. Error: queue not found:")
			},
		},
		{
			name: "purge invalid last message ID",
			override: func(p *dlqTestParams) {
				p.command = "purge"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "purge client err",
			override: func(p *dlqTestParams) {
				p.command = "purge"
				p.lastMessageID = "100"
				p.adminClient.err = errors.New("some error")
				p.expectedErrSubstrings = []string{"some error", "PurgeDLQTasks"}
			},
		},
		{
			name: "merge invalid last message ID",
			override: func(p *dlqTestParams) {
				p.command = "merge"
				p.lastMessageID = "-1"
				p.expectedErrSubstrings = []string{tdbg.FlagLastMessageID, "at least", "0"}
			},
		},
		{
			name: "merge client err",
			override: func(p *dlqTestParams) {
				p.command = "merge"
				p.lastMessageID = "100"
				p.adminClient.err = errors.New("some error")
				p.expectedErrSubstrings = []string{"some error", "MergeDLQTasks"}
			},
		},
		{
			name: "list no queues",
			override: func(p *dlqTestParams) {
				p.adminClient.err = nil
				p.command = "list"
			},
		},
		{
			name: "list queues, paginated",
			override: func(p *dlqTestParams) {
				p.adminClient.err = nil
				p.command = "list"
				p.adminClient.listQueueResponses = []*adminservice.ListQueuesResponse{
					{
						Queues: []*adminservice.ListQueuesResponse_QueueInfo{
							{
								QueueName:     "queueOne",
								MessageCount:  13,
								LastMessageId: 12, // MessageCount=13 means last message ID should be 12
							}, {
								QueueName:     "queueTwo",
								MessageCount:  42,
								LastMessageId: 41, // MessageCount=42 means last message ID should be 41
							},
						},
						NextPageToken: []byte{0x41, 0x41, 0x41},
					}, {
						Queues: []*adminservice.ListQueuesResponse_QueueInfo{
							{
								QueueName:     "queueThree",
								MessageCount:  0,
								LastMessageId: -1, // Empty queue
							},
						},
						NextPageToken: nil,
					},
				}
			},
			validateStdout: func(t *testing.T, b *bytes.Buffer) {
				// One line per queue. For my own sanity we just expect them all to exist; I'm not validating formatting or order here
				patterns := []string{`queueOne\s.*?|\s*13\b`, `queueTwo\s.*?|\s*42\b`, `queueThree\s.*?|\s*0\b`}
				for _, p := range patterns {
					assert.Regexp(t, p, b.String())
				}

			},
		},
		{
			name: "purge without last message ID",
			override: func(p *dlqTestParams) {
				p.command = "purge"
				p.lastMessageID = "" // No last message ID provided
				p.adminClient.err = nil
			},
		},
		{
			name: "merge without last message ID",
			override: func(p *dlqTestParams) {
				p.command = "merge"
				p.lastMessageID = "" // No last message ID provided
				p.adminClient.err = nil
				// Set up mock ListQueues response with our target DLQ containing LastMessageID = 150
				queueName := persistence.GetHistoryTaskQueueName(tasks.CategoryTransfer.ID(), "test-source-cluster", "test-target-cluster")
				p.adminClient.listQueueResponses = []*adminservice.ListQueuesResponse{
					{
						Queues: []*adminservice.ListQueuesResponse_QueueInfo{
							{
								QueueName:     queueName,
								MessageCount:  10,
								LastMessageId: 150,
							},
						},
						NextPageToken: nil,
					},
				}
			},
			validateStdout: func(t *testing.T, b *bytes.Buffer) {
				output := b.String()
				assert.Contains(t, output, "Note: No last message ID provided")
				assert.Contains(t, output, "Found last message ID: 150")
				assert.Contains(t, output, "upper bound for merge operation")
			},
		},
		{
			name: "purge without last message ID",
			override: func(p *dlqTestParams) {
				p.command = "purge"
				p.lastMessageID = "" // No last message ID provided
				p.adminClient.err = nil
			},
		},
		{
			name: "merge without last message ID - empty DLQ",
			override: func(p *dlqTestParams) {
				p.command = "merge"
				p.lastMessageID = "" // No last message ID provided
				p.adminClient.err = nil
				// Set up mock ListQueues response with empty queue (LastMessageID = -1)
				queueName := persistence.GetHistoryTaskQueueName(tasks.CategoryTransfer.ID(), "test-source-cluster", "test-target-cluster")
				p.adminClient.listQueueResponses = []*adminservice.ListQueuesResponse{
					{
						Queues: []*adminservice.ListQueuesResponse_QueueInfo{
							{
								QueueName:     queueName,
								MessageCount:  0,
								LastMessageId: -1, // Empty queue
							},
						},
						NextPageToken: nil,
					},
				}
			},
			validateStdout: func(t *testing.T, b *bytes.Buffer) {
				output := b.String()
				assert.Contains(t, output, "Note: No last message ID provided")
				assert.Contains(t, output, "DLQ is empty, nothing to merge")
			},
		},
		{
			name: "merge without last message ID - error while finding",
			override: func(p *dlqTestParams) {
				p.command = "merge"
				p.lastMessageID = "" // No last message ID provided
				// Set error that will be triggered when calling ListQueues
				p.adminClient.err = errors.New("connection failed")
				p.expectedErrSubstrings = []string{"failed to find last message ID", "connection failed"}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.version = "v2"
			tc.Run(t)
		})
	}
}

func (f fakeClientFactory) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	panic("not implemented")
}

func (f fakeClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return f.adminClient
}

// The fake admin client accepts and returns canned responses
// It tracks whether the correct page token is passed in the request when iterating
func (f *fakeAdminClient) ListQueues(_ context.Context, req *adminservice.ListQueuesRequest, _ ...grpc.CallOption) (*adminservice.ListQueuesResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.nextListQueueResponse >= len(f.listQueueResponses) {
		return &adminservice.ListQueuesResponse{}, nil
	}
	if bytes.Compare(f.previousPageToken, req.NextPageToken) != 0 {
		return nil, fmt.Errorf("expected page token %v, got %v", f.previousPageToken, req.NextPageToken)
	}

	resp := f.listQueueResponses[f.nextListQueueResponse]
	f.nextListQueueResponse++
	f.previousPageToken = resp.NextPageToken
	return resp, nil
}

func (f *fakeAdminClient) DescribeCluster(
	context.Context,
	*adminservice.DescribeClusterRequest,
	...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	// We _must_ return a non-nil result if the error isn't nil
	return &adminservice.DescribeClusterResponse{}, nil
}

func (f *fakeAdminClient) GetDLQTasks(
	context.Context,
	*adminservice.GetDLQTasksRequest,
	...grpc.CallOption,
) (*adminservice.GetDLQTasksResponse, error) {
	if f.err != nil {
		return nil, f.err
	}

	if len(f.getDLQTasksResponses) > 0 {
		if f.nextGetDLQTasksResponse >= len(f.getDLQTasksResponses) {
			// Return empty response if we've exhausted all responses
			return &adminservice.GetDLQTasksResponse{}, nil
		}
		response := f.getDLQTasksResponses[f.nextGetDLQTasksResponse]
		f.nextGetDLQTasksResponse++
		return response, nil
	}

	return &adminservice.GetDLQTasksResponse{}, nil
}

func (f *fakeAdminClient) PurgeDLQTasks(
	context.Context,
	*adminservice.PurgeDLQTasksRequest,
	...grpc.CallOption,
) (*adminservice.PurgeDLQTasksResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &adminservice.PurgeDLQTasksResponse{}, nil
}

func (f *fakeAdminClient) MergeDLQTasks(
	context.Context,
	*adminservice.MergeDLQTasksRequest,
	...grpc.CallOption,
) (*adminservice.MergeDLQTasksResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &adminservice.MergeDLQTasksResponse{}, nil
}

func appendArg(args []string, name string, val string) []string {
	if val == "" {
		return args
	}
	return append(args, "--"+name, val)
}
