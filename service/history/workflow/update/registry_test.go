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
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"

	updatespb "go.temporal.io/server/api/update/v1"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/workflow/update"
)

type mockUpdateStore struct {
	update.Store
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
		reg = update.NewRegistry(func() update.Store { return store })
	)
	_, ok := reg.Find(ctx, updateID)
	require.False(t, ok)

	_, found, err := reg.FindOrCreate(ctx, updateID)
	require.NoError(t, err)
	require.False(t, found)

	_, ok = reg.Find(ctx, updateID)
	require.True(t, ok)
}

func TestHasOutgoingMessages(t *testing.T) {
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
		reg     = update.NewRegistry(func() update.Store { return store })
		evStore = mockEventStore{Controller: effect.Immediate(ctx)}
	)

	upd, _, err := reg.FindOrCreate(ctx, updateID)
	require.NoError(t, err)
	require.False(t, reg.HasOutgoingMessages(false))

	req := updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID},
		Input: &updatepb.Input{Name: "not_empty"},
	}
	require.NoError(t, upd.OnMessage(ctx, &req, true, evStore))
	require.True(t, reg.HasOutgoingMessages(false))

	msg := reg.Send(ctx, false, testSequencingEventID, evStore)
	require.Len(t, msg, 1)
	require.False(t, reg.HasOutgoingMessages(false))
	require.True(t, reg.HasOutgoingMessages(true))

	acptReq := updatepb.Acceptance{
		AcceptedRequest: &req,
	}

	err = upd.OnMessage(ctx, &acptReq, true, evStore)
	require.NoError(t, err)
	require.False(t, reg.HasOutgoingMessages(false))
	require.False(t, reg.HasOutgoingMessages(true))
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
		reg = update.NewRegistry(func() update.Store { return store })
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
		status, err := upd.WaitAccepted(ctx)
		require.NoError(t, err, "completed update should also be accepted")
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.Equal(t, completedOutcome, status.Outcome,
			"completed update should have an outcome")
		status, err = upd.WaitOutcome(ctx)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.NoError(t, err, "completed update should have an outcome")
		require.Equal(t, completedOutcome, status.Outcome,
			"completed update should have an outcome")
	})
	t.Run("find stored accepted", func(t *testing.T) {
		upd, found, err := reg.FindOrCreate(ctx, acceptedUpdateID)
		require.NoError(t, err)
		require.True(t, found)
		status, err := upd.WaitAccepted(ctx)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, status.Stage)
		require.Nil(t, status.Outcome)
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
		reg     = update.NewRegistry(func() update.Store { return regStore })
		effects = effect.Buffer{}
		evStore = mockEventStore{Controller: &effects}
	)

	upd, found, err := reg.FindOrCreate(ctx, storedAcceptedUpdateID)
	require.NoError(t, err)
	require.True(t, found)

	meta := updatepb.Meta{UpdateId: storedAcceptedUpdateID}
	outcome := successOutcome(t, "success!")

	err = upd.OnMessage(
		ctx,
		&updatepb.Response{Meta: &meta, Outcome: outcome},
		true,
		evStore,
	)

	require.NoError(t, err)
	require.Equal(t, 1, reg.Len(), "update should still be present in map")
	effects.Apply(ctx)
	require.Equal(t, 0, reg.Len(), "update should have been removed")
}

func TestSendMessageGathering(t *testing.T) {
	t.Parallel()
	var (
		ctx     = context.Background()
		evStore = mockEventStore{Controller: effect.Immediate(ctx)}
		reg     = update.NewRegistry(func() update.Store { return emptyUpdateStore })
	)
	updateID1, updateID2 := t.Name()+"-update-id-1", t.Name()+"-update-id-2"
	upd1, _, err := reg.FindOrCreate(ctx, updateID1)
	require.NoError(t, err)
	upd2, _, err := reg.FindOrCreate(ctx, updateID2)
	require.NoError(t, err)
	wftStartedEventID := int64(2208)

	msgs := reg.Send(ctx, false, wftStartedEventID, evStore)
	require.Empty(t, msgs)
	require.False(t, upd1.IsSent())
	require.False(t, upd2.IsSent())

	err = upd1.OnMessage(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID1},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, true, evStore)
	require.NoError(t, err)

	msgs = reg.Send(ctx, false, wftStartedEventID, evStore)
	require.Len(t, msgs, 1)
	require.True(t, upd1.IsSent())
	require.False(t, upd2.IsSent())

	msgs = reg.Send(ctx, false, wftStartedEventID, evStore)
	require.Len(t, msgs, 0)
	require.True(t, upd1.IsSent())
	require.False(t, upd2.IsSent())

	msgs = reg.Send(ctx, true, wftStartedEventID, evStore)
	require.Len(t, msgs, 1)
	require.True(t, upd1.IsSent())
	require.False(t, upd2.IsSent())

	err = upd2.OnMessage(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID2},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, true, evStore)
	require.NoError(t, err)

	msgs = reg.Send(ctx, false, wftStartedEventID, evStore)
	require.Len(t, msgs, 1)
	require.True(t, upd1.IsSent())
	require.True(t, upd2.IsSent())

	msgs = reg.Send(ctx, false, wftStartedEventID, evStore)
	require.Len(t, msgs, 0)
	require.True(t, upd1.IsSent())
	require.True(t, upd2.IsSent())

	msgs = reg.Send(ctx, true, wftStartedEventID, evStore)
	require.Len(t, msgs, 2)
	require.True(t, upd1.IsSent())
	require.True(t, upd2.IsSent())

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
			func() update.Store { return emptyUpdateStore },
			update.WithInFlightLimit(
				func() int { return limit },
			),
		)
		evStore      = mockEventStore{Controller: effect.Immediate(ctx)}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
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
	req := updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: "update1"},
		Input: &updatepb.Input{Name: "not_empty"},
	}
	require.NoError(t, upd1.OnMessage(ctx, &req, true, evStore))

	_ = upd1.Send(ctx, false, sequencingID, evStore)

	t.Run("exceed limit after send", func(t *testing.T) {
		_, _, err = reg.FindOrCreate(ctx, "update2")
		var resExh *serviceerror.ResourceExhausted
		require.ErrorAs(t, err, &resExh)
		require.Equal(t, 1, reg.Len())
	})

	rej := updatepb.Rejection{
		RejectedRequestMessageId: "update1/request",
		RejectedRequest:          &req,
		Failure: &failurepb.Failure{
			Message: "intentional failure in " + t.Name(),
		},
	}
	require.NoError(t, upd1.OnMessage(ctx, &rej, true, evStore))
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
			func() update.Store { return emptyUpdateStore },
			update.WithTotalLimit(
				func() int { return limit },
			),
		)
		evStore      = mockEventStore{Controller: effect.Immediate(ctx)}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
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
	req := updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: "update1"},
		Input: &updatepb.Input{Name: "not_empty"},
	}
	require.NoError(t, upd1.OnMessage(ctx, &req, true, evStore))

	_ = upd1.Send(ctx, false, sequencingID, evStore)

	t.Run("exceed limit after send", func(t *testing.T) {
		_, _, err = reg.FindOrCreate(ctx, "update2")
		var failedPrecon *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecon)
		require.Equal(t, 1, reg.Len())
	})

	rej := updatepb.Rejection{
		RejectedRequestMessageId: "update1/request",
		RejectedRequest:          &req,
		Failure: &failurepb.Failure{
			Message: "intentional failure in " + t.Name(),
		},
	}
	require.NoError(t, upd1.OnMessage(ctx, &rej, true, evStore))

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
	var (
		ctx               = context.Background()
		completedUpdateID = t.Name() + "-completed-update-id"
		expectError       = fmt.Errorf("expected error in %s", t.Name())
		regStore          = mockUpdateStore{
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
		reg = update.NewRegistry(func() update.Store { return regStore })
	)

	upd, found := reg.Find(ctx, completedUpdateID)
	require.True(t, found)

	_, err := upd.WaitOutcome(ctx)
	require.ErrorIs(t, expectError, err)
}

func TestRejectUnprocessed(t *testing.T) {
	var (
		ctx          = context.Background()
		evStore      = mockEventStore{Controller: effect.Immediate(ctx)}
		reg          = update.NewRegistry(func() update.Store { return emptyUpdateStore })
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)
	updateID1, updateID2, updateID3 := t.Name()+"-update-id-1", t.Name()+"-update-id-2", t.Name()+"-update-id-3"
	upd1, _, err := reg.FindOrCreate(ctx, updateID1)
	require.NoError(t, err)
	upd2, _, err := reg.FindOrCreate(ctx, updateID2)
	require.NoError(t, err)

	rejectedIDs, err := reg.RejectUnprocessed(ctx, evStore)
	require.NoError(t, err)
	require.Empty(t, rejectedIDs, "updates in stateAdmitted should not be rejected")

	err = upd1.OnMessage(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID1},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, true, evStore)
	require.NoError(t, err)
	err = upd2.OnMessage(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID2},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, true, evStore)
	require.NoError(t, err)

	rejectedIDs, err = reg.RejectUnprocessed(ctx, evStore)
	require.NoError(t, err)
	require.Empty(t, rejectedIDs, "updates in stateRequested should not be rejected")

	upd1.Send(ctx, false, sequencingID, evStore)

	rejectedIDs, err = reg.RejectUnprocessed(ctx, evStore)
	require.NoError(t, err)
	require.Len(t, rejectedIDs, 1, "only one update in stateSent should be rejected")

	upd3, _, err := reg.FindOrCreate(ctx, updateID3)
	require.NoError(t, err)
	err = upd3.OnMessage(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID3},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, true, evStore)
	require.NoError(t, err)
	upd2.Send(ctx, false, sequencingID, evStore)
	upd3.Send(ctx, false, sequencingID, evStore)

	rejectedIDs, err = reg.RejectUnprocessed(ctx, evStore)
	require.NoError(t, err)
	require.Len(t, rejectedIDs, 2, "2 updates in stateSent should be rejected")

	rejectedIDs, err = reg.RejectUnprocessed(ctx, evStore)
	require.NoError(t, err)
	require.Len(t, rejectedIDs, 0, "rejected updates shouldn't be rejected again")
}

func TestCancelIncomplete(t *testing.T) {
	var (
		ctx          = context.Background()
		evStore      = mockEventStore{Controller: effect.Immediate(ctx)}
		reg          = update.NewRegistry(func() update.Store { return emptyUpdateStore })
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)
	updateID1, updateID2, updateID3, updateID4, updateID5 := t.Name()+"-update-id-1", t.Name()+"-update-id-2", t.Name()+"-update-id-3", t.Name()+"-update-id-4", t.Name()+"-update-id-5"
	updAdmitted, _, _ := reg.FindOrCreate(ctx, updateID1)

	updRequested, _, _ := reg.FindOrCreate(ctx, updateID2)
	_ = updRequested.OnMessage(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID2},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, true, evStore)

	updSent, _, _ := reg.FindOrCreate(ctx, updateID3)
	_ = updSent.OnMessage(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID3},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, true, evStore)
	updSent.Send(ctx, false, sequencingID, evStore)

	msgRequest4 := &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID4},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}
	updAccepted, _, _ := reg.FindOrCreate(ctx, updateID4)
	_ = updAccepted.OnMessage(ctx, msgRequest4, true, evStore)
	updAccepted.Send(ctx, false, sequencingID, evStore)
	_ = updAccepted.OnMessage(ctx, &updatepb.Acceptance{
		AcceptedRequest: msgRequest4,
	}, true, evStore)

	msgRequest5 := &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID5},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}
	updCompleted, _, _ := reg.FindOrCreate(ctx, updateID5)
	_ = updCompleted.OnMessage(ctx, msgRequest5, true, evStore)
	updCompleted.Send(ctx, false, sequencingID, evStore)
	_ = updCompleted.OnMessage(ctx, &updatepb.Acceptance{
		AcceptedRequest: msgRequest4,
	}, true, evStore)
	_ = updCompleted.OnMessage(
		ctx,
		&updatepb.Response{Meta: &updatepb.Meta{UpdateId: updateID5}, Outcome: successOutcome(t, "update completed")},
		true,
		evStore)

	err := reg.CancelIncomplete(ctx, update.CancelReasonWorkflowCompleted, evStore)
	require.NoError(t, err)

	status, err := updAdmitted.WaitOutcome(ctx)
	require.NoError(t, err)
	require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
	require.Equal(t, "Workflow Update is rejected because Workflow Execution is completed.", status.Outcome.GetFailure().GetMessage())

	status, err = updRequested.WaitOutcome(ctx)
	require.NoError(t, err)
	require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
	require.Equal(t, "Workflow Update is rejected because Workflow Execution is completed.", status.Outcome.GetFailure().GetMessage())

	status, err = updSent.WaitOutcome(ctx)
	require.NoError(t, err)
	require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
	require.Equal(t, "Workflow Update is rejected because Workflow Execution is completed.", status.Outcome.GetFailure().GetMessage())

	oneMsCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
	defer cancel()
	status, err = updAccepted.WaitOutcome(oneMsCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"expected DeadlineExceeded error when workflow is completed and update is in Accepted state")
	require.Nil(t, status.Outcome)

	status, err = updCompleted.WaitOutcome(ctx)
	require.NoError(t, err)
	require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
	require.Nil(t, status.Outcome.GetFailure())
	require.NotNil(t, status.Outcome.GetSuccess())
}
