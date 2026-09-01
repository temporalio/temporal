package nexus

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/protorequire"
)

// Response fixtures, shared with dispatch_response_test.go.

func startResponse(sor *nexuspb.StartOperationResponse) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{StartOperation: sor},
			},
		},
	}
}

func syncSuccessResponse(sync *nexuspb.StartOperationResponse_Sync) *matchingservice.DispatchNexusTaskResponse {
	return startResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{SyncSuccess: sync},
	})
}

func asyncSuccessResponse(async *nexuspb.StartOperationResponse_Async) *matchingservice.DispatchNexusTaskResponse {
	return startResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_AsyncSuccess{AsyncSuccess: async},
	})
}

func operationFailureResponse(f *failurepb.Failure) *matchingservice.DispatchNexusTaskResponse {
	return startResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_Failure{Failure: f},
	})
}

func cancelResponse() *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_CancelOperation{
					CancelOperation: &nexuspb.CancelOperationResponse{},
				},
			},
		},
	}
}

// taskFailureResponse is the arm a worker reaches through RespondNexusTaskFailed.
func taskFailureResponse(f *failurepb.Failure) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{Failure: f},
	}
}

func requestTimeoutResponse() *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		},
	}
}

func handlerFailureResponse(errType string, retry enumspb.NexusHandlerErrorRetryBehavior) *matchingservice.DispatchNexusTaskResponse {
	return taskFailureResponse(&failurepb.Failure{
		Message: "handler said no",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          errType,
				RetryBehavior: retry,
			},
		},
	})
}

func applicationFailure(msg, errType string) *failurepb.Failure {
	return &failurepb.Failure{
		Message: msg,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: errType},
		},
	}
}

func canceledFailure(msg string) *failurepb.Failure {
	return &failurepb.Failure{
		Message: msg,
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
		},
	}
}

func deprecatedHandlerErrorResponse(he *nexuspb.HandlerError) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{HandlerError: he},
	}
}

func deprecatedOperationErrorResponse(opErr *nexuspb.UnsuccessfulOperationError) *matchingservice.DispatchNexusTaskResponse {
	return startResponse(&nexuspb.StartOperationResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Variant: &nexuspb.StartOperationResponse_OperationError{OperationError: opErr},
	})
}

// Tests for *BOTH* ClassifyStartOperationDispatch and ClassifyCancelOperationDispatch.
func TestClassifyOperationDispatch(t *testing.T) {
	failure := &failurepb.Failure{Message: "op failed"}
	links := []*nexuspb.Link{{Url: "http://a.test/l", Type: "t"}}
	payload := &commonpb.Payload{Data: []byte("xxx")}

	nexusCanceledFailure, err := TemporalFailureToNexusFailure(canceledFailure("canceled within Nexus"))
	require.NoError(t, err)

	cases := []struct {
		Name     string
		Response *matchingservice.DispatchNexusTaskResponse
		Want     DispatchResult
	}{
		// DispatchOutcomeSyncSuccess
		{
			Name:     "sync success",
			Response: syncSuccessResponse(&nexuspb.StartOperationResponse_Sync{Payload: payload, Links: links}),
			Want: DispatchResult{
				Outcome:         DispatchOutcomeSyncSuccess,
				OperationResult: payload,
				Links:           links,
			},
		},
		{
			Name:     "sync success (no payload, links)",
			Response: syncSuccessResponse(&nexuspb.StartOperationResponse_Sync{}),
			Want: DispatchResult{
				Outcome:         DispatchOutcomeSyncSuccess,
				OperationResult: nil,
				Links:           nil,
			},
		},

		// DispatchOutcomeAsyncSuccess
		{
			Name: "async (pick op token over token id)",
			//nolint:staticcheck // Exercising the deprecated field on purpose.
			Response: asyncSuccessResponse(&nexuspb.StartOperationResponse_Async{OperationToken: "op-token", OperationId: "old-and-busted"}),
			Want: DispatchResult{
				Outcome:        DispatchOutcomeAsyncSuccess,
				OperationToken: "op-token",
			},
		},
		{
			Name: "async (falls back to deprecated ID)",
			//nolint:staticcheck // Exercising the deprecated field on purpose.
			Response: asyncSuccessResponse(&nexuspb.StartOperationResponse_Async{OperationId: "operation-id"}),
			Want: DispatchResult{
				Outcome:        DispatchOutcomeAsyncSuccess,
				OperationToken: "operation-id",
			},
		},
		{
			Name:     "async (no token set)",
			Response: asyncSuccessResponse(&nexuspb.StartOperationResponse_Async{}),
			Want: DispatchResult{
				Outcome:        DispatchOutcomeAsyncSuccess,
				OperationToken: "",
			},
		},

		// DispatchOutcomeOperationFailure
		{
			Name:     "failure",
			Response: operationFailureResponse(failure),
			Want: DispatchResult{
				Outcome: DispatchOutcomeOperationFailure,
				Failure: failure,
			},
		},

		// DispatchOutcomeOperationFailure (using deprecated proto)
		{
			// The deprecated error response does some extra work in the conversion,
			// setting the ApplicationFailureInfo to Canceled based on the OperationState.
			Name: "failure (deprecated proto, canceled)",
			Response: deprecatedOperationErrorResponse(
				&nexuspb.UnsuccessfulOperationError{
					OperationState: string(nexus.OperationStateCanceled),
					Failure:        &nexuspb.Failure{Message: "operation canceled"},
				}),
			Want: DispatchResult{
				Outcome: DispatchOutcomeOperationFailure,
				Failure: &failurepb.Failure{
					Message: "operation canceled",
					Cause: &failurepb.Failure{
						Message: "operation canceled",
					},
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{
						CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
					},
				},
				usedDeprecatedFormat: true,
			},
		},
		{
			// The OperationStateFailed becomes an unretryable "OperationError".
			Name: "failure (deprecated proto, failed)",
			Response: deprecatedOperationErrorResponse(
				&nexuspb.UnsuccessfulOperationError{
					OperationState: string(nexus.OperationStateFailed),
					Failure:        &nexuspb.Failure{Message: "operation failed"},
				}),
			Want: DispatchResult{
				Outcome: DispatchOutcomeOperationFailure,
				Failure: &failurepb.Failure{
					Message: "operation failed",
					Cause: &failurepb.Failure{
						Message: "operation failed",
					},
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
							Type:         "OperationError",
							NonRetryable: true,
						},
					},
				},
				usedDeprecatedFormat: true,
			},
		},
		{
			// The operation state is failed, but the cause is due to a cancellation.
			// Set both ApplicationFailureInfos, for both levels of the failurepb.Failure.
			Name: "failure (deprecated proto, failed surfaces canceled info)",
			Response: deprecatedOperationErrorResponse(
				&nexuspb.UnsuccessfulOperationError{
					OperationState: string(nexus.OperationStateFailed),
					Failure:        NexusFailureToProtoFailure(nexusCanceledFailure),
				}),
			Want: DispatchResult{
				Outcome: DispatchOutcomeOperationFailure,
				Failure: &failurepb.Failure{
					Message: "canceled within Nexus",
					Cause: &failurepb.Failure{
						Message: "canceled within Nexus",
						FailureInfo: &failurepb.Failure_CanceledFailureInfo{
							CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
						},
					},
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
							Type:         "OperationError",
							NonRetryable: true,
						},
					},
				},
				usedDeprecatedFormat: true,
			},
		},
		{
			// A worker can send a failure the server cannot re-encode. The dispatch is still classified by what
			// happened to the operation -- calling the whole response unrecognized would turn a settled operation
			// into a retryable internal error -- so the failure degrades to its message.
			Name: "failure (deprecated proto, unrecognized)",
			Response: deprecatedOperationErrorResponse(
				&nexuspb.UnsuccessfulOperationError{
					OperationState: string(nexus.OperationStateFailed),
					Failure: &nexuspb.Failure{
						Message:  "unrecognized error message",
						Metadata: map[string]string{"type": failureTypeString},
						Details:  []byte("clearly not a JSON blob!"),
					},
				}),
			Want: DispatchResult{
				Outcome: DispatchOutcomeOperationFailure,
				Failure: &failurepb.Failure{
					Message: "unrecognized error message",
					Cause: &failurepb.Failure{
						Message: "unrecognized error message",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
								Type: "NexusFailure",
							},
						},
					},
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
							Type:         "OperationError",
							NonRetryable: true,
						},
					},
				},
				usedDeprecatedFormat: true,
			},
		},

		// DispatchOutcomeHandlerFailure
		{
			Name:     "handler failure",
			Response: handlerFailureResponse(string(nexus.HandlerErrorTypeBadRequest), enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED),
			Want: DispatchResult{
				Outcome: DispatchOutcomeHandlerFailure,
				Failure: &failurepb.Failure{
					Message: "handler said no",
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
							Type: "BAD_REQUEST",
						},
					},
				},
			},
		},

		// DispatchOutcomeHandlerFailure (using deprecated protos)
		{
			// A worker predating Temporal failure responses answers with the deprecated handler error. The
			// classifier re-encodes it as the Nexus handler failure the current variant carries, keeping the type
			// and retry behavior callers act on and nesting the worker's failure as the cause.
			Name: "handler failure (deprecated proto, custom retry behavior)",
			Response: deprecatedHandlerErrorResponse(&nexuspb.HandlerError{
				ErrorType:     string(nexus.HandlerErrorTypeResourceExhausted),
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
				Failure:       &nexuspb.Failure{Message: "slow down"},
			}),
			Want: DispatchResult{
				Outcome: DispatchOutcomeHandlerFailure,
				Failure: &failurepb.Failure{
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
							Type:          "RESOURCE_EXHAUSTED",
							RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
						},
					},
					Cause: &failurepb.Failure{
						Message: "slow down",
					},
				},
				usedDeprecatedFormat: true,
			},
		},
		{
			// A handler error with no failure of its own still classifies correctly.
			Name: "handler failure (deprecated proto, no message)",
			Response: deprecatedHandlerErrorResponse(&nexuspb.HandlerError{
				ErrorType: string(nexus.HandlerErrorTypeNotFound),
			}),
			Want: DispatchResult{
				Outcome: DispatchOutcomeHandlerFailure,
				Failure: &failurepb.Failure{
					Message: "",
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
							Type: "NOT_FOUND",
						},
					},
				},
				usedDeprecatedFormat: true,
			},
		},

		// DispatchOutcomeWorkerFailure
		{
			Name:     "worker error",
			Response: taskFailureResponse(applicationFailure("worker exploded", "ErrTypeStr")),
			Want: DispatchResult{
				Outcome: DispatchOutcomeWorkerFailure,
				Failure: &failurepb.Failure{
					Message: "worker exploded",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
							Type: "ErrTypeStr",
						},
					},
				},
			},
		},

		// DispatchOutcomeUnrecognized
		{
			Name:     "unrecognized (no outcome)",
			Response: &matchingservice.DispatchNexusTaskResponse{},
			Want: DispatchResult{
				Outcome: DispatchOutcomeUnrecognized,
			},
		},
		{
			Name:     "unrecognized (nil)",
			Response: nil,
			Want: DispatchResult{
				Outcome: DispatchOutcomeUnrecognized,
			},
		},
		{
			Name:     "unrecognized (cancel answer to start request)",
			Response: cancelResponse(),
			Want: DispatchResult{
				Outcome: DispatchOutcomeUnrecognized,
			},
		},
		{
			Name:     "unrecognized (no start variant)",
			Response: startResponse(&nexuspb.StartOperationResponse{}),
			Want: DispatchResult{
				Outcome: DispatchOutcomeUnrecognized,
			},
		},
		{
			Name:     "unrecognized (nil start operation)",
			Response: startResponse(nil),
			Want: DispatchResult{
				Outcome: DispatchOutcomeUnrecognized,
			},
		},
		{
			Name: "unrecognized (empty response envelope)",
			Response: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
					Response: &nexuspb.Response{},
				},
			},
			Want: DispatchResult{
				Outcome: DispatchOutcomeUnrecognized,
			},
		},
		{
			// The only fixture that reaches the start classifier with a nil Response inside a set
			// outcome arm. On the cancel side it is also the case proving the outcome arm is what
			// marks the task answered, not the message inside it.
			Name: "unrecognized (nil response envelope)",
			Response: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Response{},
			},
			Want: DispatchResult{
				Outcome: DispatchOutcomeUnrecognized,
			},
		},

		// DispatchOutcomeRequestTimeout
		{
			Name:     "request-timeout",
			Response: requestTimeoutResponse(),
			Want: DispatchResult{
				Outcome: DispatchOutcomeRequestTimeout,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			assertClassificationsMatch := func(t *testing.T, want, got DispatchResult) {
				require.Equal(t, want.Outcome, got.Outcome)
				require.Equal(t, want.OperationToken, got.OperationToken)
				require.Equal(t, want.usedDeprecatedFormat, got.usedDeprecatedFormat)

				// TIP: To get a better error message, use require.Equal(...) for the
				// specific testcase that is producing the wrong proto result.
				protorequire.DeepEqual(t, want.OperationResult, got.OperationResult)
				protorequire.DeepEqual(t, want.Failure, got.Failure)
				protorequire.DeepEqual(t, want.Links, got.Links)
			}

			t.Run("ClassifyStartOperationDispatch", func(t *testing.T) {
				got := ClassifyStartOperationDispatch(tc.Response)
				assertClassificationsMatch(t, tc.Want, got)
			})

			t.Run("ClassifyCancelOperationDispatch", func(t *testing.T) {
				// Testing the Cancel- variant is simple. Any response that sets the
				// successful Outcome variant is considered success.
				//
				// Any other type of response uses the same error handling as before.
				got := ClassifyCancelOperationDispatch(tc.Response)
				if _, ok := tc.Response.GetOutcome().(*matchingservice.DispatchNexusTaskResponse_Response); ok {
					want := DispatchResult{
						Outcome: DispatchOutcomeCancelAccepted,
					}
					assertClassificationsMatch(t, want, got)
				} else {
					assertClassificationsMatch(t, tc.Want, got)
				}
			})
		})
	}
}

// The classifier aliases the response's failure proto rather than copying it, which callers that
// convert failures in place rely on. DispatchResult.Failure documents this as part of its contract,
// and service/frontend's TemporalFailureToNexusFailureInPlace call depends on it. The table above
// compares protos by value, so only an identity assertion catches a change to cloning.
func TestClassifyStartOperationDispatch_FailureAliasesTheResponse(t *testing.T) {
	failure := applicationFailure("worker exploded", "SomeError")
	r := ClassifyStartOperationDispatch(taskFailureResponse(failure))
	require.Same(t, failure, r.Failure)
}

// The outcome tag values are what existing dashboards query, so they are pinned here rather than only
// asserted end-to-end through a handler.
func TestDispatchResultOutcomeTag(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result DispatchResult
		want   string
	}{
		{
			name:   "sync success",
			result: DispatchResult{Outcome: DispatchOutcomeSyncSuccess},
			want:   "sync_success",
		},
		{
			name:   "async success",
			result: DispatchResult{Outcome: DispatchOutcomeAsyncSuccess},
			want:   "async_success",
		},
		{
			name:   "cancel accepted",
			result: DispatchResult{Outcome: DispatchOutcomeCancelAccepted},
			want:   "success",
		},
		{
			name:   "operation failure",
			result: DispatchResult{Outcome: DispatchOutcomeOperationFailure},
			want:   "failure",
		},
		{
			// Old-format operation errors have always had their own tag value, so normalizing the
			// outcome must not merge them into "failure".
			name: "deprecated operation error",
			result: ClassifyStartOperationDispatch(deprecatedOperationErrorResponse(
				&nexuspb.UnsuccessfulOperationError{
					OperationState: string(nexus.OperationStateFailed),
					Failure:        &nexuspb.Failure{Message: "op failed"},
				})),
			want: "operation_error",
		},
		{
			name:   "request timeout",
			result: DispatchResult{Outcome: DispatchOutcomeRequestTimeout},
			want:   "handler_timeout",
		},
		{
			name:   "unrecognized",
			result: DispatchResult{Outcome: DispatchOutcomeUnrecognized},
			want:   "handler_error:EMPTY_OUTCOME",
		},
		{
			// The zero value is not a named outcome; it must still land somewhere sensible.
			name:   "zero value",
			result: DispatchResult{},
			want:   "handler_error:EMPTY_OUTCOME",
		},
		{
			name: "handler failure reports its type",
			result: ClassifyStartOperationDispatch(handlerFailureResponse(
				string(nexus.HandlerErrorTypeBadRequest),
				enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			)),
			want: "handler_error:BAD_REQUEST",
		},
		{
			name: "deprecated handler error reports its type",
			result: ClassifyStartOperationDispatch(deprecatedHandlerErrorResponse(
				&nexuspb.HandlerError{ErrorType: string(nexus.HandlerErrorTypeNotFound)})),
			want: "handler_error:NOT_FOUND",
		},
		{
			// Not a handler error, so there is no type to report and the suffix bounds to UNKNOWN.
			name: "worker failure",
			result: ClassifyStartOperationDispatch(
				taskFailureResponse(applicationFailure("worker exploded", "SomeError"))),
			want: "handler_error:UNKNOWN",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tag := tc.result.OutcomeTag()
			require.Equal(t, "outcome", tag.Key)
			require.Equal(t, tc.want, tag.Value)
		})
	}
}

func TestBoundHandlerErrorType(t *testing.T) {
	for _, spec := range []string{
		"BAD_REQUEST", "UNAUTHENTICATED", "UNAUTHORIZED", "NOT_FOUND", "REQUEST_TIMEOUT",
		"CONFLICT", "RESOURCE_EXHAUSTED", "INTERNAL", "NOT_IMPLEMENTED", "UNAVAILABLE",
		"UPSTREAM_TIMEOUT",
	} {
		require.Equal(t, spec, boundHandlerErrorType(spec), "spec types pass through verbatim")
	}
	// A worker picks this string, so it must not be able to mint new time series.
	require.Equal(t, "UNKNOWN", boundHandlerErrorType("whatever-the-worker-said"))
	require.Equal(t, "UNKNOWN", boundHandlerErrorType(""))
	require.Equal(t, "UNKNOWN", boundHandlerErrorType("bad_request"), "matching is case sensitive")
}
