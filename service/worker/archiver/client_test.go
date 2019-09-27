// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package archiver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	carchiver "github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/cadence/mocks"
)

type clientSuite struct {
	*require.Assertions
	suite.Suite

	archiverProvider   *provider.MockArchiverProvider
	historyArchiver    *carchiver.HistoryArchiverMock
	visibilityArchiver *carchiver.VisibilityArchiverMock
	metricsClient      *mmocks.Client
	metricsScope       *mmocks.Scope
	cadenceClient      *mocks.Client
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
	s.cadenceClient = &mocks.Client{}
	s.metricsClient.On("Scope", metrics.ArchiverClientScope, mock.Anything).Return(s.metricsScope).Once()
	s.client = NewClient(
		s.metricsClient,
		log.NewNoop(),
		nil,
		dynamicconfig.GetIntPropertyFn(1000),
		dynamicconfig.GetIntPropertyFn(1000),
		s.archiverProvider,
	).(*client)
	s.client.cadenceClient = s.cadenceClient
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
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			VisibilityURI: "test:///visibility/archival",
			Targets:       []archivalTarget{ArchiveTargetVisibility},
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
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveFailureCount).Once()
	s.cadenceClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetVisibility
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			VisibilityURI: "test:///visibility/archival",
			Targets:       []archivalTarget{ArchiveTargetVisibility},
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
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalFailureCount).Once()
	s.cadenceClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetVisibility
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some random error"))

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			VisibilityURI: "test:///visibility/archival",
			Targets:       []archivalTarget{ArchiveTargetVisibility},
		},
		AttemptArchiveInline: true,
	})
	s.Error(err)
	s.Nil(resp)
}

func (s *clientSuite) TestArchiveHistoryInlineSuccess() {
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.historyArchiver, nil).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:     "test:///history/archival",
			Targets: []archivalTarget{ArchiveTargetHistory},
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
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveFailureCount).Once()
	s.cadenceClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetHistory
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:     "test:///history/archival",
			Targets: []archivalTarget{ArchiveTargetHistory},
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
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientSendSignalFailureCount).Once()
	s.cadenceClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetHistory
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some random error"))

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:     "test:///history/archival",
			Targets: []archivalTarget{ArchiveTargetHistory},
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
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.cadenceClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetHistory
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []archivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
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
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveFailureCount).Once()
	s.cadenceClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 1 && v.Targets[0] == ArchiveTargetVisibility
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []archivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
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
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveFailureCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveFailureCount).Once()
	s.cadenceClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 2
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []archivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
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
	s.metricsScope.On("IncCounter", metrics.ArchiverClientHistoryInlineArchiveAttemptCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverClientVisibilityInlineArchiveAttemptCount).Once()

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []archivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.True(resp.HistoryArchivedInline)
}

func (s *clientSuite) TestArchiveSendSignal_Success() {
	s.cadenceClient.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(v ArchiveRequest) bool {
		return len(v.Targets) == 2
	}), mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	resp, err := s.client.Archive(context.Background(), &ClientRequest{
		ArchiveRequest: &ArchiveRequest{
			URI:           "test:///history/archival",
			VisibilityURI: "test:///visibility/archival",
			Targets:       []archivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
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
			Targets: []archivalTarget{3},
		},
		AttemptArchiveInline: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.False(resp.HistoryArchivedInline)
}
