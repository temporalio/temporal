package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

func TestMergeDLQMessagesHistoryContinuation(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name          string
		input, output []byte
	}{
		{name: "first page", output: []byte("next page")},
		{name: "middle page", input: []byte("previous page"), output: []byte("next page")},
		{name: "last page", input: []byte("previous page")},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			history := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
			handler := &AdminHandler{historyClient: history, logger: log.NewNoopLogger()}
			request := &adminservice.MergeDLQMessagesRequest{
				Type:    enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION,
				ShardId: 1, SourceCluster: "source", InclusiveEndMessageId: 100,
				MaximumPageSize: 2, NextPageToken: scenario.input,
			}
			history.EXPECT().MergeDLQMessages(gomock.Any(), &historyservice.MergeDLQMessagesRequest{
				Type: request.Type, ShardId: request.ShardId, SourceCluster: request.SourceCluster,
				InclusiveEndMessageId: request.InclusiveEndMessageId,
				MaximumPageSize:       request.MaximumPageSize, NextPageToken: scenario.input,
			}).Return(&historyservice.MergeDLQMessagesResponse{NextPageToken: scenario.output}, nil)
			response, err := handler.MergeDLQMessages(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, scenario.output, response.NextPageToken)
		})
	}
}

func TestMergeDLQMessagesHistoryError(t *testing.T) {
	t.Parallel()
	history := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	handler := &AdminHandler{historyClient: history, logger: log.NewNoopLogger()}
	failure := serviceerror.NewUnavailable("history unavailable")
	history.EXPECT().MergeDLQMessages(gomock.Any(), gomock.Any()).Return(nil, failure)
	response, err := handler.MergeDLQMessages(t.Context(), &adminservice.MergeDLQMessagesRequest{Type: enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION})
	require.ErrorIs(t, err, failure)
	require.Nil(t, response)
}
