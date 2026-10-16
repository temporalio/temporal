package callbacks

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testlogger"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	"go.uber.org/mock/gomock"
)

func TestNexusStageCallbackRoutingFailure(t *testing.T) {
	t.Parallel()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	logger.Expect(testlogger.Error, "failed to decode callback token")
	capture := logger.StartCapture()
	defer logger.StopCapture(capture)
	factory := common.NewMockRPCFactory(gomock.NewController(t))
	factory.EXPECT().CreateLocalFrontendHTTPClient().Return(&common.FrontendHTTPClient{}, nil)
	provider, err := HTTPCallerProviderProvider(nil, nil, factory, nil, logger, testhooks.TestHooks{}, &Config{InspectSourceHeader: func() bool { return false }})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, commonnexus.SystemCallbackURL, nil)
	require.NoError(t, err)
	request.Header.Set(commonnexus.CallbackTokenHeader, "invalid-token")
	_, err = provider(queuescommon.NamespaceIDAndDestination{NamespaceID: "namespace-id", Destination: commonnexus.SystemCallbackURL})(request)
	require.Error(t, err)
	capture.RequireContains(t, testlogger.CapturedLogPattern{Level: testlogger.Error, Message: "failed to decode callback token", Tags: map[string]any{"nexus-stage": "handler-outbound"}})
}
