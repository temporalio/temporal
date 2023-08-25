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
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"

	updatespb "go.temporal.io/server/api/update/v1"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/workflow/update"
)

type mockUpdateStore struct {
	update.UpdateStore
	VisitUpdatesFunc     func(visitor func(updID string, updInfo *updatespb.UpdateInfo))
	GetUpdateOutcomeFunc func(context.Context, string) (*updatepb.Outcome, error)
}

func (m mockUpdateStore) VisitUpdates(
	visitor func(updID string, updInfo *updatespb.UpdateInfo),
) {
	m.VisitUpdatesFunc(visitor)
}

func (m mockUpdateStore) GetUpdateOutcome(
	ctx context.Context,
	updateID string,
) (*updatepb.Outcome, error) {
	return m.GetUpdateOutcomeFunc(ctx, updateID)
}

var emptyUpdateStore = mockUpdateStore{
	VisitUpdatesFunc: func(func(updID string, updInfo *updatespb.UpdateInfo)) {
	},
	GetUpdateOutcomeFunc: func(context.Context, string) (*updatepb.Outcome, error) {
		return nil, serviceerror.NewNotFound("not found")
	},
}

func TestFind(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		updateID = t.Name() + "-update-id"
		store    = mockUpdateStore{
			VisitUpdatesFunc: func(func(updID string, updInfo *updatespb.UpdateInfo)) {
			},
			GetUpdateOutcomeFunc: func(context.Context, string) (*updatepb.Outcome, error) {
				return nil, serviceerror.NewNotFound("not found")
			},
		}
		reg = update.NewRegistry(func() update.UpdateStore { return store })
	)
	_, ok := reg.Find(ctx, updateID)
	require.False(t, ok)

	_, found, err := reg.FindOrCreate(ctx, updateID)
	require.NoError(t, err)
	require.False(t, found)

	_, ok = reg.Find(ctx, updateID)
	require.True(t, ok)
}

func TestHasOutgoing(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		updateID = t.Name() + "-update-id"
		store    = mockUpdateStore{
			VisitUpdatesFunc: func(func(updID string, updInfo *updatespb.UpdateInfo)) {
			},
			GetUpdateOutcomeFunc: func(context.Context, string) (*updatepb.Outcome, error) {
				return nil, serviceerror.NewNotFound("not found")
			},
		}
		reg = update.NewRegistry(func() update.UpdateStore { return store })
	)

	upd, _, err := reg.FindOrCreate(ctx, updateID)
	require.NoError(t, err)
	require.False(t, reg.HasOutgoing())

	evStore := mockEventStore{Controller: effect.Immediate(ctx)}
	req := updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID},
		Input: &updatepb.Input{Name: "not_empty"},
	}
	require.NoError(t, upd.OnMessage(ctx, &req, evStore))
	require.True(t, reg.HasOutgoing())
}

func TestFindOrCreate(t *testing.T) {
	t.Parallel()
	var (
		ctx               = context.Background()
		acceptedUpdateID  = t.Name() + "-accepted-update-id"
		completedUpdateID = t.Name() + "-completed-update-id"
		completedOutcome  = successOutcome(t, "success!")

		storeData = map[string]*updatespb.UpdateInfo{
			acceptedUpdateID: {
				Value: &updatespb.UpdateInfo_Acceptance{
					Acceptance: &updatespb.AcceptanceInfo{
						EventId: 120,
					},
				},
			},
			completedUpdateID: {
				Value: &updatespb.UpdateInfo_Completion{
					Completion: &updatespb.CompletionInfo{
						EventId: 123,
					},
				},
			},
		}
		// make a store with 1 accepted and 1 completed update
		store = mockUpdateStore{
			VisitUpdatesFunc: func(visitor func(updID string, updInfo *updatespb.UpdateInfo)) {
				for updID, updInfo := range storeData {
					visitor(updID, updInfo)
				}
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
		reg = update.NewRegistry(func() update.UpdateStore { return store })
	)

	t.Run("new update", func(t *testing.T) {
		updateID := "a completely new update ID"
		_, found, err := reg.FindOrCreate(ctx, updateID)
		require.NoError(t, err)
		require.False(t, found)

		_, found, err = reg.FindOrCreate(ctx, updateID)
		require.NoError(t, err)
		require.True(t, found, "second lookup for same updateID should find previous")
	})
	t.Run("find stored completed", func(t *testing.T) {
		upd, found, err := reg.FindOrCreate(ctx, completedUpdateID)
		require.NoError(t, err)
		require.True(t, found)
		acptOutcome, err := upd.WaitAccepted(ctx)
		require.NoError(t, err, "completed update should also be accepted")
		require.Equal(t, completedOutcome, acptOutcome,
			"completed update should have an outcome")
		got, err := upd.WaitOutcome(ctx)
		require.NoError(t, err, "completed update should have an outcome")
		require.Equal(t, completedOutcome, got,
			"completed update should have an outcome")
	})
	t.Run("find stored accepted", func(t *testing.T) {
		upd, found, err := reg.FindOrCreate(ctx, acceptedUpdateID)
		require.NoError(t, err)
		require.True(t, found)
		acptOutcome, err := upd.WaitAccepted(ctx)
		require.NoError(t, err)
		require.Nil(t, acptOutcome)
	})
}

func TestUpdateRemovalFromRegistry(t *testing.T) {
	t.Parallel()
	var (
		ctx                    = context.Background()
		storedAcceptedUpdateID = t.Name() + "-accepted-update-id"
		regStore               = mockUpdateStore{
			VisitUpdatesFunc: func(visitor func(updID string, updInfo *updatespb.UpdateInfo)) {
				storedAcceptedUpdateInfo := &updatespb.UpdateInfo{
					Value: &updatespb.UpdateInfo_Acceptance{
						Acceptance: &updatespb.AcceptanceInfo{
							EventId: 120,
						},
					},
				}
				visitor(storedAcceptedUpdateID, storedAcceptedUpdateInfo)
			},
		}
		reg = update.NewRegistry(func() update.UpdateStore { return regStore })
	)

	upd, found, err := reg.FindOrCreate(ctx, storedAcceptedUpdateID)
	require.NoError(t, err)
	require.True(t, found)

	var effects effect.Buffer
	evStore := mockEventStore{Controller: &effects}
	meta := updatepb.Meta{UpdateId: storedAcceptedUpdateID}
	outcome := successOutcome(t, "success!")

	err = upd.OnMessage(
		ctx,
		&updatepb.Response{Meta: &meta, Outcome: outcome},
		evStore,
	)

	require.NoError(t, err)
	require.Equal(t, 1, reg.Len(), "update should still be present in map")
	effects.Apply(ctx)
	require.Equal(t, 0, reg.Len(), "update should have been removed")
}

func TestMessageGathering(t *testing.T) {
	t.Parallel()
	var (
		ctx = context.Background()
		reg = update.NewRegistry(func() update.UpdateStore { return emptyUpdateStore })
	)
	updateID1, updateID2 := t.Name()+"-update-id-1", t.Name()+"-update-id-2"
	upd1, _, err := reg.FindOrCreate(ctx, updateID1)
	require.NoError(t, err)
	upd2, _, err := reg.FindOrCreate(ctx, updateID2)
	require.NoError(t, err)
	wftStartedEventID := int64(123)

	msgs := reg.ReadOutgoingMessages(wftStartedEventID)
	require.Empty(t, msgs)

	evStore := mockEventStore{Controller: effect.Immediate(ctx)}

	err = upd1.OnMessage(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID1},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, evStore)
	require.NoError(t, err)

	msgs = reg.ReadOutgoingMessages(wftStartedEventID)
	require.Len(t, msgs, 1)

	err = upd2.OnMessage(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID2},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, evStore)
	require.NoError(t, err)

	msgs = reg.ReadOutgoingMessages(wftStartedEventID)
	require.Len(t, msgs, 2)

	for _, msg := range msgs {
		require.Equal(t, wftStartedEventID-1, msg.GetEventId())
	}
}

func TestInFlightLimit(t *testing.T) {
	t.Parallel()
	var (
		ctx   = context.Background()
		limit = 1
		reg   = update.NewRegistry(
			func() update.UpdateStore { return emptyUpdateStore },
			update.WithInFlightLimit(
				func() int { return limit },
			),
		)
	)
	upd1, existed, err := reg.FindOrCreate(ctx, "update1")
	require.NoError(t, err)
	require.False(t, existed)
	require.Equal(t, 1, reg.Len())

	t.Run("exceed limit", func(t *testing.T) {
		_, _, err = reg.FindOrCreate(ctx, "update2")
		var resExh *serviceerror.ResourceExhausted
		require.ErrorAs(t, err, &resExh)
		require.Equal(t, 1, reg.Len())
	})

	// complete update1 so that it is removed from the registry
	evStore := mockEventStore{Controller: effect.Immediate(ctx)}
	req := updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: "update1"},
		Input: &updatepb.Input{Name: "not_empty"},
	}
	rej := updatepb.Rejection{
		RejectedRequestMessageId: "update1/request",
		RejectedRequest:          &req,
		Failure: &failurepb.Failure{
			Message: "intentional failure in " + t.Name(),
		},
	}
	require.NoError(t, upd1.OnMessage(ctx, &req, evStore))
	require.NoError(t, upd1.OnMessage(ctx, &rej, evStore))
	require.Equal(t, 0, reg.Len(),
		"completed update should have been removed from registry")

	t.Run("admit next after returning below limit", func(t *testing.T) {
		_, existed, err = reg.FindOrCreate(ctx, "update2")
		require.NoError(t, err,
			"second update should be admitted after first completed")
		require.False(t, existed)
	})

	t.Log("Increasing limit to 2; Update registry should honor the change")
	limit = 2

	t.Run("runtime limit increase is respected", func(t *testing.T) {
		require.Equal(t, 1, reg.Len(),
			"update2 from previous test should still be in registry")
		_, existed, err := reg.FindOrCreate(ctx, "update3")
		require.NoError(t, err,
			"update should have been admitted under new higher limit")
		require.False(t, existed)
		require.Equal(t, 2, reg.Len())

		_, _, err = reg.FindOrCreate(ctx, "update4")
		var resExh *serviceerror.ResourceExhausted
		require.ErrorAs(t, err, &resExh,
			"third update should be rejected when limit = 2")
		require.Equal(t, 2, reg.Len())
	})
}

func TestTotalLimit(t *testing.T) {
	t.Parallel()
	var (
		ctx   = context.Background()
		limit = 1
		reg   = update.NewRegistry(
			func() update.UpdateStore { return emptyUpdateStore },
			update.WithTotalLimit(
				func() int { return limit },
			),
		)
	)
	upd1, existed, err := reg.FindOrCreate(ctx, "update1")
	require.NoError(t, err)
	require.False(t, existed)
	require.Equal(t, 1, reg.Len())

	t.Run("exceed limit", func(t *testing.T) {
		_, _, err = reg.FindOrCreate(ctx, "update2")
		var failedPrecon *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecon)
		require.Equal(t, 1, reg.Len())
	})

	// complete update1 so that it is removed from the registry and incremented counter
	evStore := mockEventStore{Controller: effect.Immediate(ctx)}
	req := updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: "update1"},
		Input: &updatepb.Input{Name: "not_empty"},
	}
	rej := updatepb.Rejection{
		RejectedRequestMessageId: "update1/request",
		RejectedRequest:          &req,
		Failure: &failurepb.Failure{
			Message: "intentional failure in " + t.Name(),
		},
	}
	require.NoError(t, upd1.OnMessage(ctx, &req, evStore))
	require.NoError(t, upd1.OnMessage(ctx, &rej, evStore))

	t.Run("try to admit next after completing previous", func(t *testing.T) {
		_, existed, err = reg.FindOrCreate(ctx, "update2")
		var failedPrecon *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecon)
		require.Equal(t, 0, reg.Len())
	})

	t.Log("Increasing limit to 2; Update registry should honor the change")
	limit = 2

	t.Run("runtime limit increase is respected", func(t *testing.T) {
		require.Equal(t, 0, reg.Len(),
			"registry should be empty")
		_, existed, err := reg.FindOrCreate(ctx, "update2")
		require.NoError(t, err,
			"update2 should have been admitted under new higher limit")
		require.False(t, existed)
		require.Equal(t, 1, reg.Len())

		_, _, err = reg.FindOrCreate(ctx, "update3")
		var failedPrecon *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecon,
			"update3 should be rejected when limit = 2")
		require.Equal(t, 1, reg.Len())
	})
}

func TestStorageErrorWhenLookingUpCompletedOutcome(t *testing.T) {
	t.Parallel()
	completedUpdateID := t.Name() + "-completed-update-id"
	expectError := fmt.Errorf("expected error in %s", t.Name())
	regStore := mockUpdateStore{
		VisitUpdatesFunc: func(visitor func(updID string, updInfo *updatespb.UpdateInfo)) {
			completedUpdateInfo := &updatespb.UpdateInfo{
				Value: &updatespb.UpdateInfo_Completion{
					Completion: &updatespb.CompletionInfo{EventId: 123},
				},
			}
			visitor(completedUpdateID, completedUpdateInfo)
		},
		GetUpdateOutcomeFunc: func(
			ctx context.Context,
			updateID string,
		) (*updatepb.Outcome, error) {
			return nil, expectError
		},
	}
	reg := update.NewRegistry(func() update.UpdateStore { return regStore })
	upd, found := reg.Find(context.TODO(), completedUpdateID)
	require.True(t, found)
	_, err := upd.WaitOutcome(context.TODO())
	require.ErrorIs(t, expectError, err)
}

func mustMarshalAny(t *testing.T, pb proto.Message) *types.Any {
	t.Helper()
	a, err := types.MarshalAny(pb)
	require.NoError(t, err)
	return a
}
