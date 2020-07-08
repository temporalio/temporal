// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"go.temporal.io/temporal/mocks"

	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	mmocks "go.temporal.io/server/common/metrics/mocks"
	"go.temporal.io/server/common/service/dynamicconfig"
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
			HistoryURI: "test:///history/archival",
			Targets:    []ArchivalTarget{ArchiveTargetHistory},
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
			HistoryURI: "test:///history/archival",
			Targets:    []ArchivalTarget{ArchiveTargetHistory},
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
			HistoryURI: "test:///history/archival",
			Targets:    []ArchivalTarget{ArchiveTargetHistory},
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
			HistoryURI:    "test:///history/archival",
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
			HistoryURI:    "test:///history/archival",
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
			HistoryURI:    "test:///history/archival",
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
			HistoryURI:    "test:///history/archival",
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
			HistoryURI:    "test:///history/archival",
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
