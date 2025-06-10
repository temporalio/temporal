package update_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	. "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/payloads"
	. "go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	testAcceptedEventID   int64 = 1234
	testSequencingEventID int64 = 2203
	includeAlreadySent          = true
	skipAlreadySent             = false
)

var (
	rejectionFailure = &failurepb.Failure{Message: "rejection failure"}
	rejectionOutcome = &updatepb.Outcome{Value: &updatepb.Outcome_Failure{Failure: rejectionFailure}}
	failureOutcome   = &updatepb.Outcome{Value: &updatepb.Outcome_Failure{Failure: &failurepb.Failure{Message: "outcome failure"}}}
	successOutcome   = &updatepb.Outcome{Value: &updatepb.Outcome_Success{Success: payloads.EncodeString("success")}}
	abortedOutcome   = &updatepb.Outcome{Value: &updatepb.Outcome_Failure{Failure: update.AbortFailure}}
	immediateTimeout = time.Duration(0)
	immediateCtx, _  = context.WithTimeout(context.Background(), immediateTimeout)
)

func TestUpdateState(t *testing.T) {
	type stateTest struct {
		title        string
		apply        func()
		failOnEffect bool
	}

	var (
		tv            = testvars.New(t)
		completed     bool
		upd           *update.Update
		effects       *effect.Buffer
		store         mockEventStore
		readonlyStore mockEventStore
	)

	initTest := func() {
		t.Helper()
		upd = nil // test will initialize it
		completed = false
		effects = &effect.Buffer{}
		store = mockEventStore{Controller: effects}
		readonlyStore = mockEventStore{
			Controller:      effects,
			CanAddEventFunc: func() bool { return false }, // ie workflow is already complete
		}
	}

	runTests := func(t *testing.T, initState func(), tests []*stateTest) {
		t.Helper()
		for _, tc := range tests {
			t.Run(tc.title, func(t *testing.T) {
				initTest()
				initState()
				if tc.failOnEffect == true {
					// any state transition would fail now
					store.Controller = nil
				}
				tc.apply()
			})
		}
	}

	t.Run("in stateCreated", func(t *testing.T) {
		runTests(t,
			func() {
				upd = update.New(tv.UpdateID(), update.ObserveCompletion(&completed))
			},
			[]*stateTest{{
				title: "send back status admitted to waiters requesting any pre-accepted stage",
				apply: func() {
					assertAdmitted(t, upd)
				},
			}, {
				title: "time out waiters requesting any stage post-admitted",
				apply: func() {
					assertNotAcceptedYet(t, upd)
					assertNotCompletedYet(t, upd)
				},
			}, {
				title: "transition to stateAdmitted",
				apply: func() {
					err := admit(t, store, upd)
					require.NoError(t, err)
					effects.Apply(context.Background())

					err = respondSuccess(t, store, upd)
					require.ErrorContains(t, err, "received *update.Response message while in state Admitted")
				},
			}, {
				title:        "fail to transition to stateAdmitted on invalid request",
				failOnEffect: true,
				apply: func() {
					var invalidArg *serviceerror.InvalidArgument

					err := upd.Admit(nil, nil)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "body is not set")

					err = upd.Admit(&updatepb.Request{}, nil)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "meta is not set")

					err = upd.Admit(&updatepb.Request{Meta: &updatepb.Meta{}}, nil)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "meta.update_id is not set")

					err = upd.Admit(&updatepb.Request{Meta: &updatepb.Meta{UpdateId: tv.UpdateID()}}, nil)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "input is not set")

					err = upd.Admit(&updatepb.Request{Meta: &updatepb.Meta{UpdateId: tv.UpdateID()}, Input: &updatepb.Input{}}, nil)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "input.name is not set")
				},
			}, {
				title: "fail to transition to stateAdmitted on store write failure",
				apply: func() {
					err := admit(t, store, upd)
					require.NoError(t, err)

					err = accept(t, store, upd)
					require.ErrorContains(t, err, "received *update.Acceptance message while in state ProvisionallyAdmitted")

					// rollback (ie store write failure)
					effects.Cancel(context.Background())

					err = accept(t, store, upd)
					require.ErrorContains(t, err, "received *update.Acceptance message while in state Created")
				},
			}, {
				title:        "fail to transition to stateAdmitted when workflow is already complete",
				failOnEffect: true,
				apply: func() {
					// start waiter
					ch := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 100*time.Millisecond)
						if err != nil {
							ch <- err
							return
						}
						ch <- status.Outcome
					}()

					err := admit(t, readonlyStore, upd) // NOTE the store!
					require.ErrorIs(t, update.AbortedByWorkflowClosingErr, err)
					effects.Apply(context.Background())

					// ensure waiter received response
					waiterRes := <-ch
					require.EqualExportedValues(t, update.AbortedByWorkflowClosingErr, waiterRes)

					// new waiter receives same response
					_, err = upd.WaitLifecycleStage(immediateCtx, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, immediateTimeout)
					require.ErrorIs(t, err, update.AbortedByWorkflowClosingErr)
				},
			}, {
				title:        "fail to transition to stateSent",
				failOnEffect: true,
				apply: func() {
					require.Nil(t, send(t, upd, includeAlreadySent), "update should not be sent")
				},
			}, {
				title:        "fail to transition to stateAccepted",
				failOnEffect: true,
				apply: func() {
					err := accept(t, store, upd)
					require.ErrorContains(t, err, "received *update.Acceptance message while in state Created")
				},
			}, {
				title:        "fail to transition to stateRejected",
				failOnEffect: true,
				apply: func() {
					err := reject(t, store, upd)
					require.ErrorContains(t, err, "received *update.Rejection message while in state Created")
				},
			}, {
				title:        "fail to transition to stateCompleted",
				failOnEffect: true,
				apply: func() {
					err := respondSuccess(t, store, upd)
					require.ErrorContains(t, err, "received *update.Response message while in state Created")
				},
			}, {
				title: "aborted because registry is cleared",
				apply: func() {
					abort(t, store, upd, update.AbortReasonRegistryCleared)
					effects.Apply(context.Background())
					assertAborted(t, upd, update.AbortedByServerErr)
				},
			}, {
				title: "aborted because Workflow completed",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowCompleted)
					effects.Apply(context.Background())
					assertAborted(t, upd, update.AbortedByWorkflowClosingErr)
				},
			}, {
				title: "aborted because Workflow completing",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowContinuing)
					effects.Apply(context.Background())
					assertAborted(t, upd, consts.ErrWorkflowClosing)
				},
			},
			})
	})

	t.Run("in stateAdmitted", func(t *testing.T) {
		runTests(t,
			func() {
				t.Helper()
				upd = update.New(tv.UpdateID(), update.ObserveCompletion(&completed))
				require.NoError(t, admit(t, store, upd))
				effects.Apply(context.Background())
			},
			[]*stateTest{{
				title: "send back status admitted to waiters requesting any pre-accepted stage",
				apply: func() {
					assertAdmitted(t, upd)
				},
			}, {
				title: "time out waiters requesting any stage post-admitted",
				apply: func() {
					assertNotAcceptedYet(t, upd)
					assertNotCompletedYet(t, upd)
				},
			}, {
				title:        "transition to stateSent",
				failOnEffect: true,
				apply: func() {
					require.NotNil(t, send(t, upd, skipAlreadySent), "update should be sent")

					err := respondSuccess(t, store, upd)
					require.ErrorContains(t, err, "received *update.Response message while in state Sent")
				},
			}, {
				// See comment in onAcceptanceMsg about stateAdmitted.
				// This test is to ensure that transition from stateAdmitted to stateAccepted.
				title: "transition to stateAccepted",
				apply: func() {
					// NOTE: skipping transition to stateSent!
					err := accept(t, store, upd)
					require.NoError(t, err)

					assertAdmitted(t, upd)

					// apply pending effect
					effects.Apply(context.Background())

					assertAccepted(t, upd)
					assertNotCompletedYet(t, upd)
				},
			}, {
				// See comment in onAcceptanceMsg about stateAdmitted.
				// This test is to ensure that transition from stateAdmitted to stateCompleted (rejected) is valid.
				title: "transition to stateRejected",
				apply: func() {
					// NOTE: skipping transition to stateSent!
					err := reject(t, store, upd)
					require.NoError(t, err)

					assertNotAcceptedYet(t, upd)

					// apply pending effect
					effects.Apply(context.Background())
					require.True(t, completed, "update state should now be completed")

					assertCompleted(t, upd, rejectionOutcome)
				},
			}, {
				title:        "fail to transition to stateCompleted",
				failOnEffect: true,
				apply: func() {
					err := respondSuccess(t, store, upd)
					require.ErrorContains(t, err, "received *update.Response message while in state Admitted")
				},
			}, {
				title:        "ignore another admit request",
				failOnEffect: true,
				apply: func() {
					require.NoError(t, admit(t, store, upd))
				},
			}, {
				title: "aborted because registry is cleared",
				apply: func() {
					abort(t, store, upd, update.AbortReasonRegistryCleared)
					effects.Apply(context.Background())
					assertAborted(t, upd, update.AbortedByServerErr)
				},
			}, {
				title: "aborted because Workflow completed",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowCompleted)
					effects.Apply(context.Background())
					assertAborted(t, upd, update.AbortedByWorkflowClosingErr)
				},
			}, {
				title: "aborted because Workflow completing",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowContinuing)
					effects.Apply(context.Background())
					assertAborted(t, upd, consts.ErrWorkflowClosing)
				},
			},
			})
	})

	t.Run("in stateSent", func(t *testing.T) {
		runTests(t,
			func() {
				t.Helper()
				upd = update.New(tv.UpdateID(), update.ObserveCompletion(&completed))
				require.NoError(t, admit(t, store, upd))
				effects.Apply(context.Background())
				require.NotNil(t, send(t, upd, includeAlreadySent), "update should be sent")
			},
			[]*stateTest{{
				title: "send back status admitted to waiters requesting any pre-accepted stage",
				apply: func() {
					assertAdmitted(t, upd)
				},
			}, {
				title: "time out waiters requesting any stage post-admitted",
				apply: func() {
					assertNotAcceptedYet(t, upd)
					assertNotCompletedYet(t, upd)
				},
			}, {
				title: "transition to stateAccepted",
				apply: func() {
					// start waiter
					ch := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 100*time.Millisecond)
						if err != nil {
							ch <- err
							return
						}
						ch <- status
					}()

					var gotAcceptedRequestSequencingEventId int64
					store.AddWorkflowExecutionUpdateAcceptedEventFunc = func(
						updateID string,
						acceptedRequestMessageId string,
						acceptedRequestSequencingEventId int64,
						acceptedRequest *updatepb.Request,
					) (*historypb.HistoryEvent, error) {
						gotAcceptedRequestSequencingEventId = acceptedRequestSequencingEventId
						return &historypb.HistoryEvent{}, nil
					}

					err := accept(t, store, upd)
					require.NoError(t, err)
					require.Equal(t, testSequencingEventID, gotAcceptedRequestSequencingEventId)
					assertNotAcceptedYet(t, upd)

					// apply pending effect
					effects.Apply(context.Background())

					// ensure waiter received response
					waiterRes := <-ch
					require.EqualExportedValues(t, &update.Status{Stage: UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED}, waiterRes)
				},
			}, {
				title: "fail to transition to stateAccepted/stateCompleted on store write failure",
				apply: func() {
					// acceptance and response message arrived *together*
					err := accept(t, store, upd)
					require.NoError(t, err)
					err = respondSuccess(t, store, upd)
					require.NoError(t, err)

					// rollback (ie store write failure)
					effects.Cancel(context.Background())

					assertAdmitted(t, upd)
				},
			}, {
				title:        "fail to transition to stateAccepted on invalid request",
				failOnEffect: true,
				apply: func() {
					var invalidArg *serviceerror.InvalidArgument

					err := upd.OnProtocolMessage(&protocolpb.Message{
						Body: MarshalAny(t, &updatepb.Acceptance{
							AcceptedRequestSequencingEventId: testSequencingEventID,
						}),
					}, store)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "accepted_request_message_id is not set")

					err = upd.OnProtocolMessage(&protocolpb.Message{
						Body: MarshalAny(t, &updatepb.Acceptance{
							AcceptedRequestMessageId: tv.MessageID(),
						}),
					}, store)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "accepted_request_sequencing_event_id is not set")
				},
			}, {
				title: "fail to transition to stateAccepted when workflow is already complete",
				apply: func() {
					// start waiter
					ch := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 100*time.Millisecond)
						if err != nil {
							ch <- err
							return
						}
						ch <- status.Outcome
					}()

					// accept update
					err := accept(t, readonlyStore, upd) // NOTE the store!
					require.NoError(t, err)
					effects.Apply(context.Background())

					status, err := upd.WaitLifecycleStage(immediateCtx, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, immediateTimeout)
					require.ErrorIs(t, err, update.AbortedByWorkflowClosingErr)
					require.Nil(t, status)

					// ensure waiter received response
					waiterRes := <-ch
					require.EqualExportedValues(t, update.AbortedByWorkflowClosingErr, waiterRes)

					// new waiter still sees same result
					_, err = upd.WaitLifecycleStage(immediateCtx, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, immediateTimeout)
					require.EqualExportedValues(t, update.AbortedByWorkflowClosingErr, err)
				},
			}, {
				title: "transition to stateRejected",
				apply: func() {
					// start waiter
					ch := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 100*time.Millisecond)
						if err != nil {
							ch <- err
							return
						}
						ch <- status.Outcome
					}()

					// reject update
					err := reject(t, store, upd)
					require.NoError(t, err)
					assertNotCompletedYet(t, upd)

					// apply pending effect
					effects.Apply(context.Background())
					require.True(t, completed, "update state should now be completed")

					// ensure waiter received response
					waiterRes := <-ch
					require.EqualExportedValues(t, rejectionOutcome, waiterRes)

					// new waiter receives same response
					assertCompleted(t, upd, rejectionOutcome)
				},
			}, {
				title: "fail to transition to stateRejected on store write failure",
				apply: func() {
					err := reject(t, store, upd)
					require.NoError(t, err)

					// rollback (ie store write failure)
					effects.Cancel(context.Background())

					assertAdmitted(t, upd)
				},
			}, {
				title: "fail to transition to stateRejected when workflow is already complete",
				apply: func() {
					// start waiter
					ch := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 100*time.Millisecond)
						if err != nil {
							ch <- err
							return
						}
						ch <- status.Outcome
					}()

					// reject update
					err := reject(t, readonlyStore, upd) // NOTE the store!
					require.NoError(t, err)

					// apply pending effect
					effects.Apply(context.Background())
					require.True(t, completed, "update state should now be completed")

					// ensure waiter received response
					waiterRes := <-ch
					require.EqualExportedValues(t, rejectionOutcome, waiterRes)

					// new waiter receives same response
					assertCompleted(t, upd, rejectionOutcome)
				},
			}, {
				// it is possible for the acceptance message and the completion message to
				// be part of the same message batch, and thus they will be delivered without
				// an intermediate call to apply pending effects
				title: "transition to stateCompleted via stateAccepted in one WFT",
				apply: func() {
					// start waiters
					accptCh := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 100*time.Millisecond)
						if err != nil {
							accptCh <- err
							return
						}
						accptCh <- status.Outcome
					}()
					complCh := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 100*time.Millisecond)
						if err != nil {
							complCh <- err
							return
						}
						complCh <- status.Outcome
					}()

					err := accept(t, store, upd)
					require.NoError(t, err)

					// NOTE: no call to apply pending effects between these messages!

					err = respondSuccess(t, store, upd)
					require.NoError(t, err)
					require.False(t, completed, "update state should not be completed yet")

					assertNotAcceptedYet(t, upd)
					assertNotCompletedYet(t, upd)

					// apply pending effect
					effects.Apply(context.Background())
					require.True(t, completed, "update state should now be completed")

					// ensure both waiter received completed response
					accptWaiterRes := <-accptCh
					complWaiterRes := <-complCh
					require.EqualExportedValues(t, successOutcome, accptWaiterRes)
					require.EqualExportedValues(t, successOutcome, complWaiterRes)

					// new waiter receives same response
					assertCompleted(t, upd, successOutcome)
				},
			}, {
				// it is possible for the acceptance message and the WF completion command to
				// be part of the same message batch, and thus they will be delivered without
				// an intermediate call to apply pending effects
				title: "transition to stateAborted via stateAccepted in one WFT",
				apply: func() {
					// start waiters
					accptCh := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 100*time.Millisecond)
						if err != nil {
							accptCh <- err
							return
						}
						accptCh <- status.Outcome
					}()
					complCh := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 100*time.Millisecond)
						if err != nil {
							complCh <- err
							return
						}
						complCh <- status.Outcome
					}()

					err := accept(t, store, upd)
					require.NoError(t, err)

					// NOTE: no call to apply pending effects between these messages!

					abort(t, store, upd, update.AbortReasonWorkflowCompleted)
					require.False(t, completed, "completed call back should not be called when aborted")

					assertNotAcceptedYet(t, upd)
					assertNotCompletedYet(t, upd)

					// apply pending effect
					effects.Apply(context.Background())
					require.False(t, completed, "completed call back should not be called when aborted")

					// ensure both waiter received completed response
					accptWaiterRes := <-accptCh
					complWaiterRes := <-complCh
					require.EqualExportedValues(t, abortedOutcome, accptWaiterRes)
					require.EqualExportedValues(t, abortedOutcome, complWaiterRes)

					// new waiter receives same response
					assertAborted(t, upd, nil)
				},
			}, {
				title:        "ignore another send request; unless explicitly requested",
				failOnEffect: true,
				apply: func() {
					require.Nil(t, send(t, upd, skipAlreadySent), "update should not be sent")
					require.NotNil(t, send(t, upd, includeAlreadySent), "update should be sent")
				},
			}, {
				title: "aborted because registry is cleared",
				apply: func() {
					abort(t, store, upd, update.AbortReasonRegistryCleared)
					effects.Apply(context.Background())
					assertAborted(t, upd, update.AbortedByServerErr)
				},
			}, {
				title: "aborted because Workflow completed",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowCompleted)
					effects.Apply(context.Background())
					assertAborted(t, upd, update.AbortedByWorkflowClosingErr)
				},
			}, {
				title: "aborted because Workflow completing",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowContinuing)
					effects.Apply(context.Background())
					assertAborted(t, upd, consts.ErrWorkflowClosing)
				},
			},
			})
	})

	t.Run("in stateAccepted", func(t *testing.T) {
		runTests(t,
			func() {
				t.Helper()
				upd = update.NewAccepted(tv.UpdateID(), testAcceptedEventID, update.ObserveCompletion(&completed))
			},
			[]*stateTest{{
				title: "send back status accepted to waiters requesting any pre-completed stage",
				apply: func() {
					assertAccepted(t, upd)
				},
			}, {
				title: "time out waiters requesting stage completed",
				apply: func() {
					assertNotCompletedYet(t, upd)
				},
			}, {
				title: "transition to stateCompleted with success response",
				apply: func() {
					// start waiter
					ch := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 100*time.Millisecond)
						if err != nil {
							ch <- err
							return
						}
						ch <- status.Outcome
					}()

					var gotAcceptedEventID int64
					store.AddWorkflowExecutionUpdateCompletedEventFunc = func(
						acceptedEventID int64,
						resp *updatepb.Response,
					) (*historypb.HistoryEvent, error) {
						gotAcceptedEventID = acceptedEventID
						return &historypb.HistoryEvent{}, nil
					}

					// receive update response
					err := respondSuccess(t, store, upd)
					require.NoError(t, err)
					require.False(t, completed, "update state should not be completed yet")
					require.Equal(t, testAcceptedEventID, gotAcceptedEventID)

					assertNotCompletedYet(t, upd)

					// apply pending effect
					effects.Apply(context.Background())
					require.True(t, completed, "update state should now be completed")

					// ensure waiter received response
					waiterRes := <-ch
					require.EqualExportedValues(t, successOutcome, waiterRes)

					// new waiter receives same response
					assertCompleted(t, upd, successOutcome)
				},
			}, {
				title: "transition to stateCompleted with failure response",
				apply: func() {
					// start waiter
					ch := make(chan any, 1)
					go func() {
						status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 100*time.Millisecond)
						if err != nil {
							ch <- err
							return
						}
						ch <- status.Outcome
					}()

					// receive update response
					err := respondFailure(t, store, upd)
					require.NoError(t, err)
					require.False(t, completed, "update state should not be completed yet")

					assertNotCompletedYet(t, upd)

					// apply pending effect
					effects.Apply(context.Background())
					require.True(t, completed, "update state should now be completed")

					// ensure waiter received response
					waiterRes := <-ch
					require.EqualExportedValues(t, failureOutcome, waiterRes)

					// new waiter receives same response
					assertCompleted(t, upd, failureOutcome)
				},
			}, {
				title:        "fail to transition to stateComplete on invalid request",
				failOnEffect: true,
				apply: func() {
					var invalidArg *serviceerror.InvalidArgument

					err := upd.OnProtocolMessage(&protocolpb.Message{
						Body: MarshalAny(t, &updatepb.Response{}),
					}, store)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "meta is not set")

					err = upd.OnProtocolMessage(&protocolpb.Message{
						Body: MarshalAny(t, &updatepb.Response{
							Outcome: failureOutcome,
						}),
					}, store)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "meta is not set")

					err = upd.OnProtocolMessage(&protocolpb.Message{
						Body: MarshalAny(t, &updatepb.Response{
							Meta: &updatepb.Meta{},
						}),
					}, store)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "update_id is not set")

					err = upd.OnProtocolMessage(&protocolpb.Message{
						Body: MarshalAny(t, &updatepb.Response{
							Meta: &updatepb.Meta{UpdateId: upd.ID()},
						}),
					}, store)
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "outcome is not set")
				},
			}, {
				title:        "fail to transition to stateRejected",
				failOnEffect: true,
				apply: func() {
					err := reject(t, store, upd)

					var invalidArg *serviceerror.InvalidArgument
					require.ErrorAs(t, err, &invalidArg)
					require.ErrorContains(t, err, "received *update.Rejection message while in state Accepted")
				},
			}, {
				title:        "fail to transition to stateCompleted when workflow is already complete",
				failOnEffect: true,
				apply: func() {
					err := respondSuccess(t, readonlyStore, upd) // NOTE the store!
					require.NoError(t, err)
					effects.Apply(context.Background())

					status, err := upd.WaitLifecycleStage(immediateCtx, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, immediateTimeout)
					require.NoError(t, err)
					require.NotNil(t, status)
					require.Equal(t, "Workflow Update failed because the Workflow completed before the Update completed.", status.Outcome.GetFailure().Message)
				},
			}, {
				title: "fail to transition to stateCompleted on store write failure",
				apply: func() {
					err := respondSuccess(t, store, upd)
					require.NoError(t, err)

					assertAccepted(t, upd)
					assertNotCompletedYet(t, upd)

					// rollback (ie store write failure)
					effects.Cancel(context.Background())

					assertAccepted(t, upd)
					assertNotCompletedYet(t, upd)
				},
			}, {
				title:        "ignore another admit request",
				failOnEffect: true,
				apply: func() {
					require.NoError(t, admit(t, store, upd))
				},
			}, {
				title:        "ignore another send request; even when explicitly requested",
				failOnEffect: true,
				apply: func() {
					require.Nil(t, send(t, upd, skipAlreadySent), "update should not be sent")
					require.Nil(t, send(t, upd, includeAlreadySent), "update should not be sent")
				},
			}, {
				title: "aborted because registry is cleared",
				apply: func() {
					abort(t, store, upd, update.AbortReasonRegistryCleared)
					effects.Apply(context.Background())
					// Because Update was already accepted, clear registry doesn't require retry.
					assertAccepted(t, upd)

					// And even completed stage returns accepted status (as most advanced status reached).
					status, err := upd.WaitLifecycleStage(immediateCtx, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, immediateTimeout)
					require.NoError(t, err)
					require.Equal(t, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, status.Stage)
					require.Nil(t, status.Outcome)
				},
			}, {
				title: "aborted because Workflow completed",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowCompleted)
					effects.Apply(context.Background())
					assertAborted(t, upd, nil)
				},
			}, {
				title: "aborted because Workflow completing",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowContinuing)
					effects.Apply(context.Background())
					assertAborted(t, upd, nil)
				},
			},
			})
	})

	t.Run("in stateRejected", func(t *testing.T) {
		runTests(t,
			func() {
				t.Helper()
				upd = update.New(tv.UpdateID(), update.ObserveCompletion(&completed))
				require.NoError(t, admit(t, store, upd))
				effects.Apply(context.Background())
				require.NotNil(t, send(t, upd, includeAlreadySent), "update should be sent")
				require.NoError(t, reject(t, store, upd))
				effects.Apply(context.Background())
			},
			[]*stateTest{{
				title: "send back status completed to waiters immediately for any stage",
				apply: func() {
					assertCompleted(t, upd, rejectionOutcome)
				},
			}, {
				title: "aborted because registry is cleared",
				apply: func() {
					abort(t, store, upd, update.AbortReasonRegistryCleared)
					effects.Apply(context.Background())
					assertCompleted(t, upd, rejectionOutcome)
				},
			}, {
				title: "aborted because Workflow completed",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowCompleted)
					effects.Apply(context.Background())
					assertCompleted(t, upd, rejectionOutcome)
				},
			}, {
				title: "aborted because Workflow completing",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowContinuing)
					effects.Apply(context.Background())
					assertCompleted(t, upd, rejectionOutcome)
				},
			},
			})
	})

	t.Run("in stateCompleted", func(t *testing.T) {
		runTests(t,
			func() {
				t.Helper()
				upd = update.NewCompleted(tv.UpdateID(), future.NewReadyFuture(successOutcome, nil))
				assertCompleted(t, upd, successOutcome)
			},
			[]*stateTest{{
				title: "send back status completed to waiters immediately for any stage",
				apply: func() {
					for _, stage := range []UpdateWorkflowExecutionLifecycleStage{
						UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED,
						UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED,
						UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
						UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
					} {
						t.Logf("testing stage %v", UpdateWorkflowExecutionLifecycleStage.String(stage))
						status, err := upd.WaitLifecycleStage(immediateCtx, stage, immediateTimeout)
						require.NoError(t, err)
						require.Equal(t, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
						require.EqualExportedValues(t, successOutcome, status.Outcome)
					}
				},
			}, {
				title:        "ignore another admit request",
				failOnEffect: true,
				apply: func() {
					require.NoError(t, admit(t, store, upd))
				},
			}, {
				title:        "ignore another send request; even when explicitly requested",
				failOnEffect: true,
				apply: func() {
					require.Nil(t, send(t, upd, skipAlreadySent), "update should not be sent")
					require.Nil(t, send(t, upd, includeAlreadySent), "update should not be sent")
				},
			}, {
				title: "aborted because registry is cleared",
				apply: func() {
					abort(t, store, upd, update.AbortReasonRegistryCleared)
					effects.Apply(context.Background())
					assertCompleted(t, upd, successOutcome)
				},
			}, {
				title: "aborted because Workflow completed",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowCompleted)
					effects.Apply(context.Background())
					assertCompleted(t, upd, successOutcome)
				},
			}, {
				title: "aborted because Workflow completing",
				apply: func() {
					abort(t, store, upd, update.AbortReasonWorkflowContinuing)
					effects.Apply(context.Background())
					assertCompleted(t, upd, successOutcome)
				},
			},
			})
	})
}

func TestOnProtocolMessage(t *testing.T) {
	t.Parallel()

	var tv = testvars.New(t)

	// various state transitions are already tested above, this only tests special edge cases

	t.Run("junk message", func(t *testing.T) {
		upd := update.New(tv.UpdateID())
		err := upd.OnProtocolMessage(&protocolpb.Message{
			Body: &anypb.Any{
				TypeUrl: "nonsense",
				Value:   []byte("even more nonsense"),
			},
		}, nil)
		require.Error(t, err)
	})

	t.Run("nil message", func(t *testing.T) {
		upd := update.New(tv.UpdateID())
		err := upd.OnProtocolMessage(nil, mockEventStore{})

		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "received nil message")
	})

	t.Run("nil message body", func(t *testing.T) {
		upd := update.New(tv.UpdateID())
		emptyMsg := &protocolpb.Message{}
		err := upd.OnProtocolMessage(emptyMsg, mockEventStore{})

		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "nil body")
	})

	t.Run("unsupported message type", func(t *testing.T) {
		upd := update.New(tv.UpdateID())
		msg := protocolpb.Message{}
		msg.Body = MarshalAny(t, &historypb.HistoryEvent{})
		err := upd.OnProtocolMessage(&msg, mockEventStore{})

		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "not supported")
	})
}

func mustAdmit(t *testing.T, store mockEventStore, upd *update.Update) {
	t.Helper()
	require.NoError(t, admit(t, store, upd))
	assertAdmitted(t, upd)
}

func assertAdmitted(t *testing.T, upd *update.Update) {
	t.Helper()

	status, err := upd.WaitLifecycleStage(immediateCtx, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, immediateTimeout)
	require.NoError(t, err)
	require.Equal(t, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, status.Stage)
	require.Nil(t, status.Outcome, "outcome should not be available yet")

	status, err = upd.WaitLifecycleStage(immediateCtx, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, immediateTimeout)
	require.NoError(t, err)
	require.Equal(t, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, status.Stage)
	require.Nil(t, status.Outcome, "outcome should not be available yet")
}

func admit(t *testing.T, store mockEventStore, upd *update.Update) error {
	t.Helper()
	return upd.Admit(&updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: upd.ID()},
		Input: &updatepb.Input{Name: "not_empty"},
	}, store)
}

func send(t *testing.T, upd *update.Update, includeAlreadySent bool) *protocolpb.Message {
	t.Helper()
	return upd.Send(includeAlreadySent, &protocolpb.Message_EventId{EventId: testSequencingEventID})
}

func reject(t *testing.T, store mockEventStore, upd *update.Update) error {
	t.Helper()
	return upd.OnProtocolMessage(&protocolpb.Message{
		Body: MarshalAny(t, &updatepb.Rejection{
			RejectedRequestMessageId: "update1/request",
			RejectedRequest: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: upd.ID()},
				Input: &updatepb.Input{Name: "not_empty"},
			},
			Failure: rejectionFailure,
		})}, store)
}

func mustAccept(t *testing.T, store mockEventStore, upd *update.Update) {
	t.Helper()
	require.NoError(t, accept(t, store, upd), "update should be accepted")
	assertAccepted(t, upd)
}

func accept(t *testing.T, store mockEventStore, upd *update.Update) error {
	tv := testvars.New(t)
	return upd.OnProtocolMessage(&protocolpb.Message{
		Body: MarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         tv.MessageID(),
			AcceptedRequestSequencingEventId: testSequencingEventID,
		})}, store)
}

func assertAccepted(t *testing.T, upd *update.Update) {
	t.Helper()

	for _, stage := range []UpdateWorkflowExecutionLifecycleStage{
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED,
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED,
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
	} {
		t.Logf("testing stage %v", UpdateWorkflowExecutionLifecycleStage.String(stage))
		status, err := upd.WaitLifecycleStage(immediateCtx, stage, immediateTimeout)
		require.NoError(t, err)
		require.Equal(t, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, status.Stage)
		require.Nil(t, status.Outcome, "outcome should not be available yet")
	}
}

func assertNotAcceptedYet(t *testing.T, upd *update.Update) {
	t.Helper()

	// on client timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	t.Cleanup(cancel)
	_, err := upd.WaitLifecycleStage(ctx, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 100*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// on server timeout
	status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, immediateTimeout)
	require.NoError(t, err)
	require.Equal(t, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, status.Stage)
	require.Nil(t, status.Outcome, "outcome should not be available yet")

}

func respondSuccess(t *testing.T, store mockEventStore, upd *update.Update) error {
	t.Helper()
	return upd.OnProtocolMessage(&protocolpb.Message{
		Body: MarshalAny(t, &updatepb.Response{
			Meta:    &updatepb.Meta{UpdateId: upd.ID()},
			Outcome: successOutcome,
		})}, store)
}

func respondFailure(t *testing.T, store mockEventStore, upd *update.Update) error {
	t.Helper()
	return upd.OnProtocolMessage(&protocolpb.Message{
		Body: MarshalAny(t, &updatepb.Response{
			Meta:    &updatepb.Meta{UpdateId: upd.ID()},
			Outcome: failureOutcome,
		})}, store)
}

func assertCompleted(t *testing.T, upd *update.Update, outcome *updatepb.Outcome) {
	t.Helper()

	for _, stage := range []UpdateWorkflowExecutionLifecycleStage{
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED,
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED,
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
	} {
		t.Logf("testing stage %v", UpdateWorkflowExecutionLifecycleStage.String(stage))
		status, err := upd.WaitLifecycleStage(immediateCtx, stage, immediateTimeout)
		require.NoError(t, err)
		require.Equal(t, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.EqualExportedValues(t, outcome, status.Outcome)
	}
}

func assertNotCompletedYet(t *testing.T, upd *update.Update) {
	t.Helper()

	// on client timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	t.Cleanup(cancel)
	_, err := upd.WaitLifecycleStage(ctx, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 100*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// on server timeout
	status, err := upd.WaitLifecycleStage(context.Background(), UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, immediateTimeout)
	require.NoError(t, err)
	require.NotEqual(t, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
	require.Nil(t, status.Outcome, "outcome should not be available yet")
}

func abort(t *testing.T, store mockEventStore, upd *update.Update, reason update.AbortReason) {
	t.Helper()
	upd.Abort(reason, store)
}

func assertAborted(t *testing.T, upd *update.Update, expectedErr error) {
	t.Helper()

	for _, stage := range []UpdateWorkflowExecutionLifecycleStage{
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED,
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED,
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
		UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
	} {
		t.Logf("testing stage %v", UpdateWorkflowExecutionLifecycleStage.String(stage))
		status, err := upd.WaitLifecycleStage(immediateCtx, stage, immediateTimeout)
		if expectedErr == nil {
			// expectedErr == nil means update was aborted with failure instead of error.
			require.NoError(t, err)
			require.Equal(t, UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
			require.EqualExportedValues(t, abortedOutcome, status.Outcome)
		} else {
			require.Error(t, err)
			require.ErrorIs(t, err, expectedErr)
		}
	}
}
