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

package update_test

import (
	"context"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/workflow/update"
)

var (
	emptyUpdateStore = mockUpdateStore{
		VisitUpdatesFunc: func(func(updID string, updInfo *persistencespb.UpdateInfo)) {},
		GetUpdateOutcomeFunc: func(context.Context, string) (*updatepb.Outcome, error) {
			return nil, serviceerror.NewNotFound("not found")
		},
	}
	eventStoreUnused update.EventStore
)

type mockUpdateStore struct {
	update.UpdateStore
	VisitUpdatesFunc               func(visitor func(updID string, updInfo *persistencespb.UpdateInfo))
	GetUpdateOutcomeFunc           func(context.Context, string) (*updatepb.Outcome, error)
	GetCurrentVersionFunc          func() int64
	IsWorkflowExecutionRunningFunc func() bool
}

func (m mockUpdateStore) VisitUpdates(
	visitor func(updID string, updInfo *persistencespb.UpdateInfo),
) {
	if m.VisitUpdatesFunc != nil {
		m.VisitUpdatesFunc(visitor)
	}
}

func (m mockUpdateStore) GetUpdateOutcome(
	ctx context.Context,
	updateID string,
) (*updatepb.Outcome, error) {
	return m.GetUpdateOutcomeFunc(ctx, updateID)
}

func (m mockUpdateStore) GetCurrentVersion() int64 {
	if m.GetCurrentVersionFunc == nil {
		return 0
	}
	return m.GetCurrentVersionFunc()
}

func (m mockUpdateStore) IsWorkflowExecutionRunning() bool {
	if m.IsWorkflowExecutionRunningFunc == nil {
		return true
	}
	return m.IsWorkflowExecutionRunningFunc()
}

type mockEventStore struct {
	effect.Controller
	AddWorkflowExecutionUpdateAcceptedEventFunc func(
		updateID string,
		acceptedRequestMessageId string,
		acceptedRequestSequencingEventId int64,
		acceptedRequest *updatepb.Request,
	) (*historypb.HistoryEvent, error)

	AddWorkflowExecutionUpdateCompletedEventFunc func(
		acceptedEventID int64,
		resp *updatepb.Response,
	) (*historypb.HistoryEvent, error)

	CanAddEventFunc func() bool
}

func (m mockEventStore) AddWorkflowExecutionUpdateAcceptedEvent(
	updateID string,
	acceptedRequestMessageId string,
	acceptedRequestSequencingEventId int64,
	acceptedRequest *updatepb.Request,
) (*historypb.HistoryEvent, error) {
	if m.AddWorkflowExecutionUpdateAcceptedEventFunc != nil {
		return m.AddWorkflowExecutionUpdateAcceptedEventFunc(updateID, acceptedRequestMessageId, acceptedRequestSequencingEventId, acceptedRequest)
	}
	return &historypb.HistoryEvent{EventId: testAcceptedEventID}, nil
}

func (m mockEventStore) AddWorkflowExecutionUpdateCompletedEvent(
	acceptedEventID int64,
	resp *updatepb.Response,
) (*historypb.HistoryEvent, error) {
	if m.AddWorkflowExecutionUpdateCompletedEventFunc != nil {
		return m.AddWorkflowExecutionUpdateCompletedEventFunc(acceptedEventID, resp)
	}
	return &historypb.HistoryEvent{}, nil
}

func (m mockEventStore) CanAddEvent() bool {
	if m.CanAddEventFunc != nil {
		return m.CanAddEventFunc()
	}
	return true
}
