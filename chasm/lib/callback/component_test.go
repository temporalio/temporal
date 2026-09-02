package callback

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFromAPICallback(t *testing.T) {
	// Set of API Callback variants to test.
	apiCallbackVariants := map[string]struct {
		callback *commonpb.Callback
		// Whether CHASM can persist this variant. Persistable variants must round trip.
		persistable bool
	}{
		"nexus": {
			callback: &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url:    "http://localhost:8080/cb",
						Header: map[string]string{"key": "value"},
					},
				},
			},
			persistable: true,
		},
		"nexus handler": {
			callback: &commonpb.Callback{
				Variant: &commonpb.Callback_NexusHandler_{
					NexusHandler: &commonpb.Callback_NexusHandler{
						TaskQueueName: "completions-task-queue",
						Service:       "HTTPAdapter",
						Operation:     "DeliverAsWebhook",
						SourceContext: &commonpb.Payload{Data: []byte("...")},
					},
				},
			},
			persistable: true,
		},
		"internal": {
			callback: &commonpb.Callback{
				Variant: &commonpb.Callback_Internal_{Internal: &commonpb.Callback_Internal{}},
			},
			// Not defined in the CHASM callback proto.
			persistable: false,
		},
		"unset": {callback: &commonpb.Callback{}},
	}

	t.Run("RoundTripped", func(t *testing.T) {
		for name, tc := range apiCallbackVariants {
			t.Run(name, func(t *testing.T) {
				got, err := FromAPICallback(tc.callback)

				// Error case, for invalid or unknown API callbacks.
				if !tc.persistable {
					var invalidArgErr *serviceerror.InvalidArgument
					require.ErrorAs(t, err, &invalidArgErr)
					require.ErrorContains(t, err, "unsupported callback variant")
					return
				}
				require.NoError(t, err)

				// Verify round-tripping the proto produces the same result.
				chasmComponent := &Callback{
					CallbackState: &callbackspb.CallbackState{
						Callback: got,
					},
				}
				roundTripped, err := chasmComponent.ToAPICallback()
				require.NoError(t, err)
				protorequire.ProtoEqual(t, tc.callback, roundTripped)
			})
		}
	})

	t.Run("LinksPersistedForVariants", func(t *testing.T) {
		links := []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "wf-id"},
				},
			},
			{
				Variant: &commonpb.Link_Callback_{
					Callback: &commonpb.Link_Callback{
						Execution: &commonpb.Execution{
							Type:       enumspb.EXECUTION_TYPE_NEXUS_OPERATION,
							BusinessId: "nexus-operation-id",
							RunId:      "run-id",
						},
						RequestId: "request-id",
					},
				},
			},
		}

		for name, tc := range apiCallbackVariants {
			// This test only applies to callbacks that can be converted.
			if !tc.persistable {
				continue
			}

			t.Run(name, func(t *testing.T) {
				cbWithLinks := common.CloneProto(tc.callback)
				cbWithLinks.Links = links

				got, err := FromAPICallback(cbWithLinks)
				require.NoError(t, err)

				// Verify links were converted in the process.
				gotLinks := got.GetLinks()
				require.Len(t, gotLinks, 2)
				require.NotNil(t, gotLinks[0].GetWorkflowEvent())
				require.NotNil(t, gotLinks[1].GetCallback())

				// Verify that a deep copy was used. (Different references.)
				require.NotSame(t, links[0], gotLinks[0])
				require.NotSame(t, links[1], gotLinks[1])
			})
		}
	})
}

// Asserts that CHASM Callbacks do not support the new NexusHandler callback variant.
func TestNexusHandlerCallbacksNotSupported(t *testing.T) {
	apiCb := &commonpb.Callback{
		Variant: &commonpb.Callback_NexusHandler_{
			NexusHandler: &commonpb.Callback_NexusHandler{},
		},
	}
	chasmCB, err := FromAPICallback(apiCb)
	require.NoError(t, err)

	cb := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: chasmCB,
		},
	}
	_, err = cb.loadInvocationArgs(&chasm.MockMutableContext{}, nil)

	var unprocessableErr *queueserrors.UnprocessableTaskError
	require.ErrorAs(t, err, &unprocessableErr)
	require.ErrorContains(t, err, "unprocessable callback variant")
	require.ErrorContains(t, err, "Callback_NexusHandler_")
}

// Verify the setResult method sets the "result" field based on the Callback state.
func TestSetResult(t *testing.T) {
	lastAttemptFailure := &failurepb.Failure{Message: "last attempt"}

	cases := []struct {
		name string

		// Callback state to set.
		status             callbackspb.CallbackStatus
		lastAttemptFailure *failurepb.Failure

		// The CallbackInfo expected after setResult.
		want *callbackpb.CallbackInfo
	}{
		{
			name:   "unspecified is non-terminal",
			status: callbackspb.CALLBACK_STATUS_UNSPECIFIED,
			want:   &callbackpb.CallbackInfo{},
		},
		{
			name:   "standby is non-terminal",
			status: callbackspb.CALLBACK_STATUS_STANDBY,
			want:   &callbackpb.CallbackInfo{},
		},
		{
			name:   "scheduled is non-terminal",
			status: callbackspb.CALLBACK_STATUS_SCHEDULED,
			want:   &callbackpb.CallbackInfo{},
		},
		{
			name:               "backing off is non-terminal, even with a last attempt failure",
			status:             callbackspb.CALLBACK_STATUS_BACKING_OFF,
			lastAttemptFailure: lastAttemptFailure,
			want:               &callbackpb.CallbackInfo{},
		},
		{
			name:   "succeeded",
			status: callbackspb.CALLBACK_STATUS_SUCCEEDED,
			want: &callbackpb.CallbackInfo{
				Result: &callbackpb.CallbackInfo_Success{Success: &emptypb.Empty{}},
			},
		},
		{
			name:               "failed reports the terminal failure",
			status:             callbackspb.CALLBACK_STATUS_FAILED,
			lastAttemptFailure: lastAttemptFailure,
			want: &callbackpb.CallbackInfo{
				Result: &callbackpb.CallbackInfo_Failure{Failure: lastAttemptFailure},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := &Callback{
				CallbackState: &callbackspb.CallbackState{
					Status:             tc.status,
					LastAttemptFailure: tc.lastAttemptFailure,
				},
			}

			var cbInfo callbackpb.CallbackInfo
			cb.setResult(&cbInfo)

			protorequire.ProtoEqual(t, tc.want, &cbInfo)
			if gotFailure := cbInfo.GetFailure(); gotFailure != nil {
				require.NotSame(t, tc.lastAttemptFailure, gotFailure)
			}
		})
	}
}

const (
	testNamespaceID = "test-namespace-id"
	testCallbackURL = "http://callback.example.com:8080/path?query=string"
	// testDestination is what the outbound queue keys the callback's rate limiter and circuit
	// breaker by: the scheme and host of the callback URL. See [callbackDestination].
	testDestination = "http://callback.example.com:8080"
)

// blockedQuery is one question the component asked the injected DestinationBlockedFn.
type blockedQuery struct {
	namespaceID string
	destination string
}

// destinationBlockedStub is a fake for the History Service's outbound queue's circuit breakers.
// It keeps track of the blocked/unblocked state.
type destinationBlockedStub struct {
	blocked bool
	queries []blockedQuery
}

func (s *destinationBlockedStub) fn(namespaceID string, destination string) bool {
	s.queries = append(s.queries, blockedQuery{namespaceID: namespaceID, destination: destination})
	return s.blocked
}

func newTestContext(t *testing.T, lib *Library) chasm.Context {
	t.Helper()

	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(lib))

	backend := &chasm.MockNodeBackend{
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey(testNamespaceID, "business-id", "run-id")
		},
	}
	node := chasm.NewEmptyTree(
		registry,
		clock.NewEventTimeSource(),
		backend,
		chasm.DefaultPathEncoder,
		logger,
		metrics.NoopMetricsHandler,
	)
	return chasm.NewContext(context.Background(), node)
}

// newTestLibrary builds the library the way fx does, sans task handlers.
func newTestLibrary(destinationBlocked DestinationBlockedFn) *Library {
	return newLibrary(libraryParams{DestinationBlocked: destinationBlocked})
}

func newTestCallback(status callbackspb.CallbackStatus, variant *callbackspb.Callback) *Callback {
	return &Callback{
		CallbackState: &callbackspb.CallbackState{
			Status:   status,
			Callback: variant,
		},
	}
}

func nexusVariant(url string) *callbackspb.Callback {
	return &callbackspb.Callback{
		Variant: &callbackspb.Callback_Nexus_{Nexus: &callbackspb.Callback_Nexus{Url: url}},
	}
}

// Only a SCHEDULED callback can be held back by an open circuit breaker: it is the one state in
// which a delivery is waiting on the outbound queue. Every other state reports as itself and must
// not consult the breaker at all.
func TestAPIStateCircuitBreaker(t *testing.T) {
	cases := []struct {
		name      string
		status    callbackspb.CallbackStatus
		wantState enumspb.CallbackState
		// blockable is true for the states an open breaker can turn into BLOCKED.
		blockable bool
	}{
		{
			name:      "standby",
			status:    callbackspb.CALLBACK_STATUS_STANDBY,
			wantState: enumspb.CALLBACK_STATE_STANDBY,
		},
		{
			name:      "scheduled",
			status:    callbackspb.CALLBACK_STATUS_SCHEDULED,
			wantState: enumspb.CALLBACK_STATE_SCHEDULED,
			blockable: true,
		},
		{
			name:      "backing off",
			status:    callbackspb.CALLBACK_STATUS_BACKING_OFF,
			wantState: enumspb.CALLBACK_STATE_BACKING_OFF,
		},
		{
			name:      "succeeded",
			status:    callbackspb.CALLBACK_STATUS_SUCCEEDED,
			wantState: enumspb.CALLBACK_STATE_SUCCEEDED,
		},
		{
			name:      "failed",
			status:    callbackspb.CALLBACK_STATUS_FAILED,
			wantState: enumspb.CALLBACK_STATE_FAILED,
		},
	}

	for _, tc := range cases {
		for _, blocked := range []bool{false, true} {
			name := tc.name
			if blocked {
				name += " with an open breaker"
			}

			t.Run(name, func(t *testing.T) {
				stub := &destinationBlockedStub{blocked: blocked}
				ctx := newTestContext(t, newTestLibrary(stub.fn))
				cb := newTestCallback(tc.status, nexusVariant(testCallbackURL))

				state, blockedReason, err := cb.APIState(ctx)
				require.NoError(t, err)

				if blocked && tc.blockable {
					require.Equal(t, enumspb.CALLBACK_STATE_BLOCKED, state)
					require.Equal(t, "The circuit breaker is open.", blockedReason)
				} else {
					require.Equal(t, tc.wantState, state)
					require.Empty(t, blockedReason)
				}

				if tc.blockable {
					require.Equal(t,
						[]blockedQuery{{namespaceID: testNamespaceID, destination: testDestination}},
						stub.queries)
				} else {
					require.Empty(t, stub.queries, "the breaker is only relevant to a scheduled callback")
				}
			})
		}
	}
}

// The breaker is keyed per (namespace, destination), so the component has to ask about the same
// destination the invocation task was scheduled with - the callback's variant decides it.
func TestAPIStateQueriesCallbackDestination(t *testing.T) {
	cases := []struct {
		name            string
		variant         *callbackspb.Callback
		wantDestination string
	}{
		{
			name:            "nexus",
			variant:         nexusVariant(testCallbackURL),
			wantDestination: testDestination,
		},
		{
			name: "nexus handler",
			variant: &callbackspb.Callback{
				Variant: &callbackspb.Callback_NexusHandler_{
					NexusHandler: &callbackspb.Callback_NexusHandler{TaskQueueName: "completions-task-queue"},
				},
			},
			wantDestination: "nexus-handler://completions-task-queue",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &destinationBlockedStub{}
			ctx := newTestContext(t, newTestLibrary(stub.fn))
			cb := newTestCallback(callbackspb.CALLBACK_STATUS_SCHEDULED, tc.variant)

			_, _, err := cb.APIState(ctx)
			require.NoError(t, err)
			require.Equal(t,
				[]blockedQuery{{namespaceID: testNamespaceID, destination: tc.wantDestination}},
				stub.queries)
		})
	}
}

// A callback whose URL cannot be parsed has no destination to ask about, and APIState reports the
// failure rather than a state.
func TestAPIStateInvalidDestination(t *testing.T) {
	stub := &destinationBlockedStub{blocked: true}
	ctx := newTestContext(t, newTestLibrary(stub.fn))
	cb := newTestCallback(callbackspb.CALLBACK_STATUS_SCHEDULED, nexusVariant("http://invalid url/path"))

	state, blockedReason, err := cb.APIState(ctx)
	require.ErrorContains(t, err, "failed to parse URL:")
	require.Equal(t, enumspb.CALLBACK_STATE_UNSPECIFIED, state)
	require.Empty(t, blockedReason)
	require.Empty(t, stub.queries)
}

// Only the history service runs the outbound queue, so elsewhere the library is built without a
// DestinationBlockedFn. Those processes must still be able to describe a callback.
func TestAPIStateWithoutInjectedDestinationBlocked(t *testing.T) {
	for name, lib := range map[string]*Library{
		"no fn provided": newTestLibrary(nil),
		"nil library":    NewNilLibrary(),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := newTestContext(t, lib)
			cb := newTestCallback(callbackspb.CALLBACK_STATUS_SCHEDULED, nexusVariant(testCallbackURL))

			state, blockedReason, err := cb.APIState(ctx)
			require.NoError(t, err)
			require.Equal(t, enumspb.CALLBACK_STATE_SCHEDULED, state)
			require.Empty(t, blockedReason)
		})
	}
}

// ToAPICallbackInfo surfaces the blocked state and its reason alongside the rest of the callback's
// state, which is what Describe returns to a caller.
func TestToAPICallbackInfoReportsBlocked(t *testing.T) {
	registrationTime := timestamppb.New(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))

	for _, blocked := range []bool{false, true} {
		name := "scheduled"
		if blocked {
			name = "blocked by an open breaker"
		}

		t.Run(name, func(t *testing.T) {
			stub := &destinationBlockedStub{blocked: blocked}
			ctx := newTestContext(t, newTestLibrary(stub.fn))

			cb := newTestCallback(callbackspb.CALLBACK_STATUS_SCHEDULED, nexusVariant(testCallbackURL))
			cb.RequestId = "request-id"
			cb.RegistrationTime = registrationTime
			cb.Attempt = 7

			info, err := cb.ToAPICallbackInfo(ctx)
			require.NoError(t, err)

			wantState := enumspb.CALLBACK_STATE_SCHEDULED
			wantReason := ""
			if blocked {
				wantState = enumspb.CALLBACK_STATE_BLOCKED
				wantReason = "The circuit breaker is open."
			}
			want := &callbackpb.CallbackInfo{
				Callback: &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{Url: testCallbackURL},
					},
				},
				RegistrationTime: registrationTime,
				State:            wantState,
				BlockedReason:    wantReason,
				RequestId:        "request-id",
				Attempt:          7,
			}

			protorequire.ProtoEqual(t, want, info)
		})
	}
}
