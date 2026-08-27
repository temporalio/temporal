package tdbg_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
)

type forceScheduleClientFactory struct {
	history historyservice.HistoryServiceClient
}

func (f forceScheduleClientFactory) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return nil
}

func (f forceScheduleClientFactory) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	return nil
}

func (f forceScheduleClientFactory) HistoryClient(*cli.Context) historyservice.HistoryServiceClient {
	return f.history
}

type forceScheduleHistoryClient struct {
	historyservice.HistoryServiceClient
	err     error
	request *historyservice.ScheduleWorkflowTaskRequest
}

func (c *forceScheduleHistoryClient) ScheduleWorkflowTask(
	_ context.Context,
	request *historyservice.ScheduleWorkflowTaskRequest,
	_ ...grpc.CallOption,
) (*historyservice.ScheduleWorkflowTaskResponse, error) {
	c.request = request
	if c.err != nil {
		return nil, c.err
	}
	return &historyservice.ScheduleWorkflowTaskResponse{}, nil
}

func runForceScheduleCommand(
	t *testing.T,
	factory tdbg.ClientFactory,
	args ...string,
) (string, error) {
	t.Helper()
	var stdout bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = factory
		params.Writer = &stdout
	})

	err := app.Run(append([]string{"tdbg"}, args...))
	return stdout.String(), err
}

func TestForceScheduleFirstWorkflowTask(t *testing.T) {
	historyClient := &forceScheduleHistoryClient{}
	stdout, err := runForceScheduleCommand(t, forceScheduleClientFactory{history: historyClient},
		"execution", "force-schedule-first-workflow-task",
		"--namespace-id", "namespace-id",
		"--workflow-id", "workflow-id",
		"--run-id", "run-id",
		"--history-address", "127.0.0.1:7233",
	)

	require.NoError(t, err)
	require.NotNil(t, historyClient.request)
	require.Equal(t, "namespace-id", historyClient.request.GetNamespaceId())
	require.Equal(t, &commonpb.WorkflowExecution{
		WorkflowId: "workflow-id",
		RunId:      "run-id",
	}, historyClient.request.GetWorkflowExecution())
	require.True(t, historyClient.request.GetIsFirstWorkflowTask())
	require.Contains(t, stdout, "First workflow task scheduled successfully.")
}

func TestForceScheduleFirstWorkflowTask_RPCError(t *testing.T) {
	rpcErr := errors.New("history unavailable")
	historyClient := &forceScheduleHistoryClient{err: rpcErr}
	_, err := runForceScheduleCommand(t, forceScheduleClientFactory{history: historyClient},
		"execution", "force-schedule-first-workflow-task",
		"--namespace-id", "namespace-id",
		"--workflow-id", "workflow-id",
		"--history-address", "127.0.0.1:7233",
	)

	require.ErrorIs(t, err, rpcErr)
	require.Contains(t, err.Error(), "unable to force schedule first workflow task")
}

func TestForceScheduleFirstWorkflowTask_RequiredFlags(t *testing.T) {
	testCases := []struct {
		name string
		args []string
	}{
		{
			name: "namespace ID",
			args: []string{
				"execution", "force-schedule-first-workflow-task",
				"--workflow-id", "workflow-id",
				"--history-address", "127.0.0.1:7233",
			},
		},
		{
			name: "workflow ID",
			args: []string{
				"execution", "force-schedule-first-workflow-task",
				"--namespace-id", "namespace-id",
				"--history-address", "127.0.0.1:7233",
			},
		},
		{
			name: "history address",
			args: []string{
				"execution", "force-schedule-first-workflow-task",
				"--namespace-id", "namespace-id",
				"--workflow-id", "workflow-id",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			historyClient := &forceScheduleHistoryClient{}
			_, err := runForceScheduleCommand(t, forceScheduleClientFactory{history: historyClient}, tc.args...)

			require.Error(t, err)
			require.Nil(t, historyClient.request)
		})
	}
}
