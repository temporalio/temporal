package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

func TestNexusEndpointClientListPersistenceErrorUsesStructuredLogFields(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	persistenceManager := persistence.NewMockNexusEndpointManager(controller)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	capture := logger.StartCapture()
	client := newNexusEndpointClient(
		&nexusEndpointClientConfig{
			listDefaultPageSize: func() int { return 100 },
			listMaxPageSize:     func() int { return 1000 },
		},
		nil,
		nil,
		persistenceManager,
		logger,
	)
	testErr := errors.New("persistence failure")
	pageToken := []byte("caller-controlled-page-token")
	persistenceManager.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		NextPageToken:         pageToken,
		PageSize:              42,
	}).Return(nil, testErr)

	response, err := client.List(context.Background(), &operatorservice.ListNexusEndpointsRequest{
		NextPageToken: pageToken,
		PageSize:      42,
	})

	require.Nil(t, response)
	var internalErr *serviceerror.Internal
	require.ErrorAs(t, err, &internalErr)
	require.Equal(t, []testlogger.CapturedLog{{
		Level:   testlogger.Error,
		Message: "error listing Nexus endpoints from persistence",
		Tags: []tag.Tag{
			tag.Error(testErr),
			tag.Binary("next-page-token", pageToken),
			tag.Int32("page-size", 42),
		},
	}}, capture.Snapshot())
}

func TestNexusEndpointClientListByNamePersistenceErrorUsesStructuredLogFields(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	persistenceManager := persistence.NewMockNexusEndpointManager(controller)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	capture := logger.StartCapture()
	client := newNexusEndpointClient(
		&nexusEndpointClientConfig{
			listDefaultPageSize: func() int { return 1 },
			listMaxPageSize:     func() int { return 1000 },
		},
		nil,
		nil,
		persistenceManager,
		logger,
	)
	testErr := errors.New("persistence failure")
	pageToken := []byte("persistence-page-token")
	persistenceManager.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		PageSize:              1,
	}).Return(&persistence.ListNexusEndpointsResponse{
		Entries: []*persistencespb.NexusEndpointEntry{{
			Endpoint: &persistencespb.NexusEndpoint{
				Spec: &persistencespb.NexusEndpointSpec{Name: "other-endpoint"},
			},
		}},
		NextPageToken: pageToken,
	}, nil)
	persistenceManager.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		NextPageToken:         pageToken,
		PageSize:              1,
	}).Return(nil, testErr)

	response, err := client.List(context.Background(), &operatorservice.ListNexusEndpointsRequest{
		Name: "caller-controlled-endpoint-name",
	})

	require.Nil(t, response)
	var internalErr *serviceerror.Internal
	require.ErrorAs(t, err, &internalErr)
	require.Equal(t, []testlogger.CapturedLog{{
		Level:   testlogger.Error,
		Message: "error listing Nexus endpoints from persistence with Name filter",
		Tags: []tag.Tag{
			tag.Error(testErr),
			tag.Binary("next-page-token", pageToken),
			tag.Int("page-size", 1),
			tag.Endpoint("caller-controlled-endpoint-name"),
		},
	}}, capture.Snapshot())
}
