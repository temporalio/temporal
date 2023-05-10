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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/workflow/update"
)

type mockUpdateStore struct {
	update.UpdateStore
	GetAcceptedWorkflowExecutionUpdateIDsFunc func(context.Context) []string
	GetUpdateInfoFunc                         func(context.Context, string) (*persistencespb.UpdateInfo, bool)
	GetUpdateOutcomeFunc                      func(context.Context, string) (*updatepb.Outcome, error)
}

func (m mockUpdateStore) GetAcceptedWorkflowExecutionUpdateIDs(
	ctx context.Context,
) []string {
	return m.GetAcceptedWorkflowExecutionUpdateIDsFunc(ctx)
}

func (m mockUpdateStore) GetUpdateInfo(
	ctx context.Context,
	updateID string,
) (*persistencespb.UpdateInfo, bool) {
	return m.GetUpdateInfoFunc(ctx, updateID)
}

func (m mockUpdateStore) GetUpdateOutcome(
	ctx context.Context,
	updateID string,
) (*updatepb.Outcome, error) {
	return m.GetUpdateOutcomeFunc(ctx, updateID)
}

func TestFind(t *testing.T) {
	t.Parallel()
	updateID := t.Name() + "-update-id"
	store := mockUpdateStore{
		GetAcceptedWorkflowExecutionUpdateIDsFunc: func(context.Context) []string {
			return nil
		},
		GetUpdateInfoFunc: func(
			ctx context.Context,
			updateID string,
		) (*persistencespb.UpdateInfo, bool) {
			return nil, false
		},
	}
	reg := update.NewRegistry(store)
	_, ok := reg.Find(context.TODO(), updateID)
	require.False(t, ok)

	_, found, err := reg.FindOrCreate(context.TODO(), updateID)
	require.NoError(t, err)
	require.False(t, found)

	_, ok = reg.Find(context.TODO(), updateID)
	require.True(t, ok)
}

func TestHasOutgoing(t *testing.T) {
	t.Parallel()
	var (
		updateID = t.Name() + "-update-id"
		store    = mockUpdateStore{
			GetAcceptedWorkflowExecutionUpdateIDsFunc: func(context.Context) []string {
				return nil
			},
			GetUpdateInfoFunc: func(
				ctx context.Context,
				updateID string,
			) (*persistencespb.UpdateInfo, bool) {
				return nil, false
			},
		}
		reg = update.NewRegistry(store)
	)

	upd, _, err := reg.FindOrCreate(context.TODO(), updateID)
	require.NoError(t, err)
	require.False(t, reg.HasOutgoing())

	evStore := mockEventStore{Controller: effect.Immediate(context.TODO())}
	req := updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID},
		Input: &updatepb.Input{Name: "not_empty"},
	}
	require.NoError(t, upd.OnMessage(context.TODO(), &req, evStore))
	require.True(t, reg.HasOutgoing())
}

func TestFindOrCreate(t *testing.T) {
	t.Parallel()
	var (
		acceptedUpdateID  = t.Name() + "-accepted-update-id"
		completedUpdateID = t.Name() + "-completed-update-id"
		completedOutcome  = successOutcome(t, "success!")

		storeData = map[string]*persistencespb.UpdateInfo{
			acceptedUpdateID: &persistencespb.UpdateInfo{
				Value: &persistencespb.UpdateInfo_AcceptancePointer{
					AcceptancePointer: &historyspb.HistoryEventPointer{EventId: 120},
				},
			},
			completedUpdateID: &persistencespb.UpdateInfo{
				Value: &persistencespb.UpdateInfo_CompletedPointer{
					CompletedPointer: &historyspb.HistoryEventPointer{EventId: 123},
				},
			},
		}
		// make a store with 1 accepted and 1 completed update
		store = mockUpdateStore{
			GetAcceptedWorkflowExecutionUpdateIDsFunc: func(context.Context) []string {
				return []string{acceptedUpdateID}
			},
			GetUpdateInfoFunc: func(
				ctx context.Context,
				updateID string,
			) (*persistencespb.UpdateInfo, bool) {
				ui, ok := storeData[updateID]
				return ui, ok
			},
			GetUpdateOutcomeFunc: func(
				ctx context.Context,
				updateID string,
			) (*updatepb.Outcome, error) {
				if updateID == completedUpdateID {
					return completedOutcome, nil
				}
				return nil, serviceerror.NewNotFound("not found")
			},
		}
		reg = update.NewRegistry(store)
	)

	require.True(t, reg.HasIncomplete())

	t.Run("new update", func(t *testing.T) {
		updateID := "a completely new update ID"
		_, found, err := reg.FindOrCreate(context.TODO(), updateID)
		require.NoError(t, err)
		require.False(t, found)

		_, found, err = reg.FindOrCreate(context.TODO(), updateID)
		require.NoError(t, err)
		require.True(t, found, "second lookup for same updateID should find previous")
	})
	t.Run("find stored completed", func(t *testing.T) {
		upd, found, err := reg.FindOrCreate(context.TODO(), completedUpdateID)
		require.NoError(t, err)
		require.True(t, found)
		acptOutcome, err := upd.WaitAccepted(context.TODO())
		require.NoError(t, err, "completed update should also be accepted")
		require.Equal(t, completedOutcome, acptOutcome, "completed update should have an outcome")
		got, err := upd.WaitOutcome(context.TODO())
		require.NoError(t, err, "completed update should have an outcome")
		require.Equal(t, completedOutcome, got, "completed update should have an outcome")
	})
	t.Run("find stored accepted", func(t *testing.T) {
		upd, found, err := reg.FindOrCreate(context.TODO(), acceptedUpdateID)
		require.NoError(t, err)
		require.True(t, found)
		acptOutcome, err := upd.WaitAccepted(context.TODO())
		require.NoError(t, err)
		require.Nil(t, acptOutcome)
	})
}

func TestUpdateRemovalFromRegistry(t *testing.T) {
	t.Parallel()
	var (
		storedAcceptedUpdateID = t.Name() + "-accepted-update-id"
		regStore               = mockUpdateStore{
			GetAcceptedWorkflowExecutionUpdateIDsFunc: func(context.Context) []string {
				return []string{storedAcceptedUpdateID}
			},
		}
		reg = update.NewRegistry(regStore)
	)

	upd, found, err := reg.FindOrCreate(context.TODO(), storedAcceptedUpdateID)
	require.NoError(t, err)
	require.True(t, found)

	var effects effect.Buffer
	evStore := mockEventStore{Controller: &effects}
	meta := updatepb.Meta{UpdateId: storedAcceptedUpdateID}
	outcome := successOutcome(t, "success!")
	require.True(t, reg.HasIncomplete(), "accepted update counts as incomplete")

	err = upd.OnMessage(context.TODO(), &updatepb.Response{Meta: &meta, Outcome: outcome}, evStore)

	require.NoError(t, err)
	require.False(t, reg.HasIncomplete(), "updates should be ProvisionallyCompleted")
	require.Equal(t, 1, reg.Len(), "update should still be present in map")
	effects.Apply(context.TODO())
	require.Equal(t, 0, reg.Len(), "update should have been removed")
}

func TestMessageGathering(t *testing.T) {
	t.Parallel()
	var (
		regStore = mockUpdateStore{
			GetAcceptedWorkflowExecutionUpdateIDsFunc: func(context.Context) []string {
				return nil
			},
			GetUpdateInfoFunc: func(context.Context, string) (*persistencespb.UpdateInfo, bool) {
				return nil, false
			},
		}
		reg = update.NewRegistry(regStore)
	)

	updateID1, updateID2 := t.Name()+"-update-id-1", t.Name()+"-update-id-2"
	upd1, _, err := reg.FindOrCreate(context.TODO(), updateID1)
	require.NoError(t, err)
	upd2, _, err := reg.FindOrCreate(context.TODO(), updateID2)
	require.NoError(t, err)
	wftStartedEventID := int64(123)

	msgs, err := reg.CreateOutgoingMessages(wftStartedEventID)
	require.NoError(t, err)
	require.Empty(t, msgs)

	evStore := mockEventStore{Controller: effect.Immediate(context.TODO())}

	err = upd1.OnMessage(context.TODO(), &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID1},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, evStore)
	require.NoError(t, err)

	msgs, err = reg.CreateOutgoingMessages(wftStartedEventID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	err = upd2.OnMessage(context.TODO(), &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID2},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, evStore)
	require.NoError(t, err)

	msgs, err = reg.CreateOutgoingMessages(wftStartedEventID)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	for _, msg := range msgs {
		require.Equal(t, wftStartedEventID-1, msg.GetEventId())
	}
}

func mustMarshalAny(t *testing.T, pb proto.Message) *types.Any {
	t.Helper()
	a, err := types.MarshalAny(pb)
	require.NoError(t, err)
	return a
}
