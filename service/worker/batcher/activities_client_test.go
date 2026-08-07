package batcher

import (
	"testing"

	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/testsuite"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.uber.org/mock/gomock"
)

// The factory tracks every client NewClient hands out and releases them only on
// shutdown, so an activity that leaves its client open retains it for the life of
// the worker process, once per execution.
func TestBatchActivityWithProtobuf_ClosesSDKClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	sdkClient := mocksdk.NewMockClient(ctrl)
	clientFactory := sdk.NewMockClientFactory(ctrl)
	clientFactory.EXPECT().NewClient(gomock.Any()).Return(sdkClient)
	sdkClient.EXPECT().
		CountWorkflow(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewInvalidArgument("bad query"))
	sdkClient.EXPECT().Close()

	a := newBoundActivities(nil)
	a.ClientFactory = clientFactory

	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(a.BatchActivityWithProtobuf)

	_, err := env.ExecuteActivity(a.BatchActivityWithProtobuf, &batchspb.BatchOperationInput{
		NamespaceId: boundNSID,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace:       boundNSName,
			VisibilityQuery: "WorkflowType='foo'",
			Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
				SignalOperation: &batchpb.BatchOperationSignal{Signal: "s"},
			},
		},
	})
	require.Error(t, err)
}
