package archiver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/temporal/mocks"

	carchiver "github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	mmocks "github.com/temporalio/temporal/common/metrics/mocks"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type clientSuite struct {
	*require.Assertions
	suite.Suite

	archiverProvider   *provider.MockArchiverProvider
	historyArchiver    *carchiver.HistoryArchiverMock
	visibilityArchiver *carchiver.VisibilityArchiverMock
	metricsClient      *mmocks.Client
	metricsScope       *mmocks.Scope
	temporalClient     *mocks.Client
	client             *client
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(clientSuite))
}

func (s *clientSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.archiverProvider = &provider.MockArchiverProvider{}
	s.historyArchiver = &carchiver.HistoryArchiverMock{}
	s.visibilityArchiver = &carchiver.VisibilityArchiverMock{}
	s.metricsClient = &mmocks.Client{}
	s.metricsScope = &mmocks.Scope{}
	s.temporalClient = &mocks.Client{}
	s.metricsClient.On("Scope", metrics.ArchiverClientScope, mock.Anything).Return(s.metricsScope).Once()
	s.client = NewClient(
		s.metricsClient,
		log.NewNoop(),
		nil,
		dynamicconfig.GetIntPropertyFn(1000),
		dynamicconfig.GetIntPropertyFn(1000),
		s.archiverProvider,
	).(*client)
	s.client.temporalClient = s.temporalClient
}

func (s *clientSuite) TearDownTest() {
	s.archiverProvider.AssertExpectations(s.T())
	s.historyArchiver.AssertExpectations(s.T())
	s.visibilityArchiver.AssertExpectations(s.T())
	s.metricsClient.AssertExpectations(s.T())
	s.metricsScope.AssertExpectations(s.T())
}

func (s *clientSuite) TestArchiveVisibilityInlineSuccess() {
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.visibilityArchiver, nil).Once()
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			VisibilityURI: "test:///visibility/archival",
			Targets:       []ArchivalTarget{ArchiveTargetVisibility},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.False(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveVisibilityInlineFail_SendSignalSuccess() {
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.visibilityArchiver, nil).Once()
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some random error")).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalCount).Once()
	s.temporalClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetVisibility
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			VisibilityURI: "test:///visibility/archival",
			Targets:       []ArchivalTarget{ArchiveTargetVisibility},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.False(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveVisibilityInlineFail_SendSignalFail() {
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.visibilityArchiver, nil).Once()
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some random error")).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalFailureCount).Once()
	s.temporalClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetVisibility
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some random error"))

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			VisibilityURI: "test:///visibility/archival",
			Targets:       []ArchivalTarget{ArchiveTargetVisibility},
		},
		AttemptArchiveInline: true,
	})
	s.Error(err)
	s.Nil(resp)
}

func (s *clientSuite) TestArchiveHistoryInlineSuccess() {
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.historyArchiver, nil).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:     "test:///history/archival",
			Targets: []ArchivalTarget{ArchiveTargetHistory},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.True(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveHistoryInlineFail_SendSignalSuccess() {
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.historyArchiver, nil).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some random error")).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalCount).Once()
	s.temporalClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetHistory
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:     "test:///history/archival",
			Targets: []ArchivalTarget{ArchiveTargetHistory},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.False(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveHistoryInlineFail_SendSignalFail() {
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.historyArchiver, nil).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some random error")).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalFailureCount).Once()
	s.temporalClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetHistory
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some random error"))

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:     "test:///history/archival",
			Targets: []ArchivalTarget{ArchiveTargetHistory},
		},
		AttemptArchiveInline: true,
	})
	s.Error(err)
	s.Nil(resp)
}

func (s *clientSuite) TestArchiveInline_HistoryFail_VisibilitySuccess() {
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.historyArchiver, nil).Once()
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.visibilityArchiver, nil).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some random error")).Once()
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalCount).Once()
	s.temporalClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetHistory
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.False(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveInline_VisibilityFail_HistorySuccess() {
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.historyArchiver, nil).Once()
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.visibilityArchiver, nil).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some random error")).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalCount).Once()
	s.temporalClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetVisibility
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.True(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveInline_VisibilityFail_HistoryFail() {
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.historyArchiver, nil).Once()
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.visibilityArchiver, nil).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some random error")).Once()
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some random error")).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalCount).Once()
	s.temporalClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 2
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.False(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveInline_VisibilitySuccess_HistorySuccess() {
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.historyArchiver, nil).Once()
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.visibilityArchiver, nil).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.True(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveSendSignal_Success() {
	s.temporalClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 2
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityRequestCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalCount).Once()

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
		},
		AttemptArchiveInline: false,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.False(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveUnknownTarget() {
	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			Targets: []ArchivalTarget{3},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.False(resp.HistoryArchivedInline)
}
