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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
)

type clientSuite struct {
	*require.Assertions
	suite.Suite

	controller *gomock.Controller

	archiverProvider   *provider.MockArchiverProvider
	historyArchiver    *carchiver.MockHistoryArchiver
	visibilityArchiver *carchiver.MockVisibilityArchiver
	metricsClient      *metrics.MockClient
	metricsScope       *metrics.MockScope
	sdkClientFactory   *sdk.MockClientFactory
	sdkClient          *mocksdk.MockClient
	client             *client
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(clientSuite))
}

func (s *clientSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.archiverProvider = provider.NewMockArchiverProvider(s.controller)
	s.historyArchiver = carchiver.NewMockHistoryArchiver(s.controller)
	s.visibilityArchiver = carchiver.NewMockVisibilityArchiver(s.controller)
	s.metricsClient = metrics.NewMockClient(s.controller)
	s.metricsScope = metrics.NewMockScope(s.controller)
	s.metricsClient.EXPECT().Scope(metrics.ArchiverClientScope, gomock.Any()).Return(s.metricsScope)
	s.sdkClient = mocksdk.NewMockClient(s.controller)
	s.sdkClientFactory = sdk.NewMockClientFactory(s.controller)
	s.sdkClientFactory.EXPECT().GetSystemClient(gomock.Any()).Return(s.sdkClient).AnyTimes()
	s.client = NewClient(
		s.metricsClient,
		log.NewNoopLogger(),
		s.sdkClientFactory,
		dynamicconfig.GetIntPropertyFn(1000),
		dynamicconfig.GetIntPropertyFn(1000),
		s.archiverProvider,
	).(*client)
}

func (s *clientSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *clientSuite) TestArchiveVisibilityInlineSuccess() {
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.visibilityArchiver, nil)
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveAttemptCount)

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
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.visibilityArchiver, nil)
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("some random error"))
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveFailureCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalCount)
	s.sdkClient.EXPECT().SignalWithStartWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), archiveRequestOneTarget(ArchiveTargetVisibility),
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

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
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.visibilityArchiver, nil)
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("some random error"))
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveFailureCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalFailureCount)
	s.sdkClient.EXPECT().SignalWithStartWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), archiveRequestOneTarget(ArchiveTargetVisibility),
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))

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
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.historyArchiver, nil)
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveAttemptCount)

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
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.historyArchiver, nil)
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("some random error"))
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveFailureCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalCount)
	s.sdkClient.EXPECT().SignalWithStartWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), archiveRequestOneTarget(ArchiveTargetHistory),
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

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
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.historyArchiver, nil)
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("some random error"))
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveFailureCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalFailureCount)
	s.sdkClient.EXPECT().SignalWithStartWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), archiveRequestOneTarget(ArchiveTargetHistory),
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))

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
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.historyArchiver, nil)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.visibilityArchiver, nil)
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("some random error"))
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveFailureCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalCount)
	s.sdkClient.EXPECT().SignalWithStartWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), archiveRequestOneTarget(ArchiveTargetHistory),
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

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
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.historyArchiver, nil)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.visibilityArchiver, nil)
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("some random error"))
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveFailureCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalCount)
	s.sdkClient.EXPECT().SignalWithStartWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), archiveRequestOneTarget(ArchiveTargetVisibility),
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

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
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.historyArchiver, nil)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.visibilityArchiver, nil)
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("some random error"))
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("some random error"))
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveFailureCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveFailureCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalCount)
	s.sdkClient.EXPECT().SignalWithStartWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), archiveRequestBothTargets{},
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

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
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(s.historyArchiver, nil)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).Return(s.visibilityArchiver, nil)
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryInlineArchiveAttemptCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityInlineArchiveAttemptCount)

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
	s.sdkClient.EXPECT().SignalWithStartWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), archiveRequestBothTargets{},
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientHistoryRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientVisibilityRequestCount)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverClientSendSignalCount)

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

type archiveRequestOneTarget ArchivalTarget

func (m archiveRequestOneTarget) Matches(x interface{}) bool {
	v, ok := x.(ArchiveRequest)
	return ok && len(v.Targets) == 1 && v.Targets[0] == ArchivalTarget(m)
}

func (m archiveRequestOneTarget) String() string {
	return fmt.Sprintf("%#v", m)
}

type archiveRequestBothTargets struct{}

func (m archiveRequestBothTargets) Matches(x interface{}) bool {
	v, ok := x.(ArchiveRequest)
	return ok && len(v.Targets) == 2
}

func (m archiveRequestBothTargets) String() string {
	return fmt.Sprintf("%#v", m)
}
