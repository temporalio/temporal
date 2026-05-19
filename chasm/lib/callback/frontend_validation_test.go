package callback

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRequiredStringFields(t *testing.T) {
	// Positive tests
	positiveTests := requiredFields{
		{"Field1", "exists"},
		{"Field2", " "}, // Whitespace, but still non-empty.
	}
	require.NoError(t, positiveTests.Validate())
	for _, positiveTest := range positiveTests {
		require.NoError(t, positiveTest.Validate())
	}

	// Negative tests
	negativeTests := requiredFields{
		{"Field1", ""},
		{"Field2", ""},
	}
	require.ErrorContains(t, negativeTests.Validate(), "Field1 is required")
	for _, negativeTest := range negativeTests {
		wantErr := fmt.Sprintf("%s is required", negativeTest.FieldName)
		require.ErrorContains(t, negativeTest.Validate(), wantErr)
	}

	// Special case: Confirm the validation error is stable, in that it
	// is always the first invalid field in the slice.
	mixedTests := requiredFields{
		{"Mixed Field1", "ok"},
		{"Mixed Field2", ""},
		{"Mixed Field3", ""},
		{"Mixed Field4", "ok"},
		{"Mixed Field5", ""},
	}
	require.ErrorContains(t, mixedTests.Validate(), "Mixed Field2 is required")
}

type noopCallbackValidator struct{}

func (noopCallbackValidator) Validate(context.Context, string, []*commonpb.Callback) error {
	return nil
}

func newTestRequestValidator(maxIDLength, blobLimit int) *frontendRequestValidator {
	return &frontendRequestValidator{
		config: &Config{
			MaxIDLength:        func() int { return maxIDLength },
			BlobSizeLimitError: func(string) int { return blobLimit },
			BlobSizeLimitWarn:  func(string) int { return blobLimit },
		},
		cbValidator: noopCallbackValidator{},
		logger:      log.NewNoopLogger(),
	}
}

type validationTest[T any] struct {
	name    string
	mutate  func(*T)
	wantErr string
}

func runValidationTests[T any](
	t *testing.T,
	newValidRequestProto func() *T,
	validate func(*T) error,
	tests []validationTest[T],
) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newValidRequestProto()
			tt.mutate(req)
			err := validate(req)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

// TestFrontendRequestValidator covers all of the exported functions from the
// frontendRequestValidator type, using the generic `runValidationTests[T]` to
// cut down on boilerplate.
func TestFrontendRequestValidator(t *testing.T) {
	const (
		maxIDLength = 64
		maxBlobSize = 1024
	)
	rv := newTestRequestValidator(maxIDLength, maxBlobSize)

	longID := strings.Repeat("a", maxIDLength+1)
	validUUID := uuid.NewString()

	t.Run("ValidateAndNormalizeStartCallbackExecution", func(t *testing.T) {
		type ReqProto = workflowservice.StartCallbackExecutionRequest
		newValidRequestProto := func() *ReqProto {
			return &ReqProto{
				Namespace:  "namespace",
				Identity:   "identity",
				CallbackId: "callback-id",
				Input: &workflowservice.StartCallbackExecutionRequest_Completion{
					Completion: &callbackpb.CallbackExecutionCompletion{
						Result: &callbackpb.CallbackExecutionCompletion_Success{Success: &commonpb.Payload{}},
					},
				},
			}
		}

		validate := func(req *ReqProto) error {
			ctx := context.Background()
			return rv.ValidateAndNormalizeStartCallbackExecution(ctx, req)
		}

		runValidationTests(t, newValidRequestProto, validate, []validationTest[ReqProto]{
			{"valid", func(*ReqProto) {}, ""},
			{"missing namespace", func(r *ReqProto) { r.Namespace = "" }, "namespace is required"},
			{"missing identity", func(r *ReqProto) { r.Identity = "" }, "identity is required"},
			{"missing callback_id", func(r *ReqProto) { r.CallbackId = "" }, "callback_id is required"},
			{"callback_id too long", func(r *ReqProto) { r.CallbackId = longID }, "callback_id exceeds length limit"},
			{"identity too long", func(r *ReqProto) { r.Identity = longID }, "identity exceeds length limit"},
			{"request_id too long", func(r *ReqProto) { r.RequestId = longID }, "request_id exceeds length limit"},
			{
				"negative schedule_to_close timeout",
				func(r *ReqProto) {
					r.ScheduleToCloseTimeout = durationpb.New(-time.Second)
				},
				"schedule_to_close_timeout is invalid",
			},
			{"missing completion", func(r *ReqProto) { r.Input = nil }, "completion is not set"},
			{
				"completion empty",
				func(r *ReqProto) {
					r.Input = &workflowservice.StartCallbackExecutionRequest_Completion{
						Completion: &callbackpb.CallbackExecutionCompletion{},
					}
				},
				"completion must have either success or failure set",
			},
			{
				"completion oversized",
				func(r *ReqProto) {
					massivePayload := strings.Repeat("x", maxBlobSize+1)
					massiveCompletion := &workflowservice.StartCallbackExecutionRequest_Completion{
						Completion: &callbackpb.CallbackExecutionCompletion{
							Result: &callbackpb.CallbackExecutionCompletion_Success{
								Success: &commonpb.Payload{
									Data: []byte(massivePayload),
								},
							},
						},
					}
					r.Input = massiveCompletion
				},
				"Blob data size exceeds limit",
			},
		})

		t.Run("RequestID is auto-assigned", func(t *testing.T) {
			req := newValidRequestProto()
			require.NoError(t, validate(req))
			require.NotEmpty(t, req.RequestId)
		})
	})

	t.Run("ValidateDescribeCallbackExecution", func(t *testing.T) {
		nsID := namespace.ID("ns-id")
		tokenFor := func(nsID string) []byte {
			b, err := proto.Marshal(&persistencespb.ChasmComponentRef{
				NamespaceId: nsID,
				BusinessId:  "callback-id",
			})
			require.NoError(t, err)
			return b
		}

		type ReqProto = workflowservice.DescribeCallbackExecutionRequest
		newValidRequestProto := func() *ReqProto {
			return &ReqProto{
				Namespace:  "ns",
				CallbackId: "cb",
			}
		}
		validate := func(req *ReqProto) error {
			return rv.ValidateDescribeCallbackExecution(req, nsID)
		}

		runValidationTests(t, newValidRequestProto, validate, []validationTest[ReqProto]{
			{"valid", func(*ReqProto) {}, ""},
			{"valid run_id", func(r *ReqProto) { r.RunId = validUUID }, ""},
			{"missing namespace", func(r *ReqProto) { r.Namespace = "" }, "namespace is required"},
			{"missing callback_id", func(r *ReqProto) { r.CallbackId = "" }, "callback_id is required"},
			{"invalid run_id", func(r *ReqProto) { r.RunId = "not-uuid" }, "invalid run_id"},
			{"callback_id too long", func(r *ReqProto) { r.CallbackId = longID }, "callback_id exceeds length limit"},
			{"long_poll without run_id", func(r *ReqProto) { r.LongPollToken = []byte("x") }, "run_id is required when long_poll_token"},
			{
				"malformed long_poll",
				func(r *ReqProto) {
					r.RunId, r.LongPollToken = validUUID, []byte("garbage")
				},
				"invalid long poll token",
			},
			{
				"long_poll namespace mismatch",
				func(r *ReqProto) {
					r.RunId, r.LongPollToken = validUUID, tokenFor("other-ns-id")
				},
				"long poll token does not match",
			},
			{
				"long_poll match",
				func(r *ReqProto) {
					r.RunId, r.LongPollToken = validUUID, tokenFor(nsID.String())
				},
				"",
			},
		})
	})

	t.Run("ValidatePollCallbackExecution", func(t *testing.T) {
		type ReqProto = workflowservice.PollCallbackExecutionRequest
		newValidRequestProto := func() *ReqProto {
			return &ReqProto{Namespace: "ns", CallbackId: "cb"}
		}

		runValidationTests(t, newValidRequestProto, rv.ValidatePollCallbackExecution, []validationTest[ReqProto]{
			{"valid", func(*ReqProto) {}, ""},
			{"valid run_id", func(r *ReqProto) { r.RunId = uuid.NewString() }, ""},
			{"missing namespace", func(r *ReqProto) { r.Namespace = "" }, "namespace is required"},
			{"missing callback_id", func(r *ReqProto) { r.CallbackId = "" }, "callback_id is required"},
			{"invalid run_id", func(r *ReqProto) { r.RunId = "not-uuid" }, "invalid run_id"},
			{"callback_id too long", func(r *ReqProto) { r.CallbackId = longID }, "callback_id exceeds length limit"},
		})
	})

	t.Run("ValidateAndNormalizeTerminateCallbackExecution", func(t *testing.T) {
		type ReqProto = workflowservice.TerminateCallbackExecutionRequest
		newValidRequestProto := func() *ReqProto {
			return &ReqProto{
				Namespace:  "ns",
				CallbackId: "callback-id",
			}
		}

		runValidationTests(t, newValidRequestProto, rv.ValidateAndNormalizeTerminateCallbackExecution, []validationTest[ReqProto]{
			{"valid", func(*ReqProto) {}, ""},
			{"missing namespace", func(r *ReqProto) { r.Namespace = "" }, "namespace is required"},
			{"missing callback_id", func(r *ReqProto) { r.CallbackId = "" }, "callback_id is required"},
			{"invalid run_id", func(r *ReqProto) { r.RunId = "not-uuid" }, "invalid run_id"},
			{"callback_id too long", func(r *ReqProto) { r.CallbackId = longID }, "callback_id exceeds length limit"},
			{"identity too long", func(r *ReqProto) { r.Identity = longID }, "identity exceeds length limit"},
			{"request_id too long", func(r *ReqProto) { r.RequestId = longID }, "request_id exceeds length limit"},
			{"reason too long", func(r *ReqProto) { r.Reason = longID }, "reason exceeds length limit"},
		})

		t.Run("RequestID is auto-assigned", func(t *testing.T) {
			req := newValidRequestProto()
			require.NoError(t, rv.ValidateAndNormalizeTerminateCallbackExecution(req))
			require.NotEmpty(t, req.RequestId)
		})
	})

	t.Run("ValidateDeleteCallbackExecution", func(t *testing.T) {
		type ReqProto = workflowservice.DeleteCallbackExecutionRequest
		newValidRequestProto := func() *ReqProto {
			return &ReqProto{
				Namespace:  "ns",
				CallbackId: "callback-id",
			}
		}

		runValidationTests(t, newValidRequestProto, rv.ValidateDeleteCallbackExecution, []validationTest[ReqProto]{
			{"valid", func(*ReqProto) {}, ""},
			{"missing namespace", func(r *ReqProto) { r.Namespace = "" }, "namespace is required"},
			{"missing callback_id", func(r *ReqProto) { r.CallbackId = "" }, "callback_id is required"},
			{"callback_id too long", func(r *ReqProto) { r.CallbackId = longID }, "callback_id exceeds length limit"},
		})
	})

	t.Run("ValidateListCallbackExecutions", func(t *testing.T) {
		// Only validates the Namespace is set.
		rv := newTestRequestValidator(10, 20)
		require.NoError(t, rv.ValidateListCallbackExecutions(&workflowservice.ListCallbackExecutionsRequest{Namespace: "ns"}))
		require.ErrorContains(t, rv.ValidateListCallbackExecutions(&workflowservice.ListCallbackExecutionsRequest{}), "namespace is required")
	})

	t.Run("ValidateCountCallbackExecutions", func(t *testing.T) {
		// Only validates the Namespace is set.
		rv := newTestRequestValidator(10, 20)
		require.NoError(t, rv.ValidateCountCallbackExecutions(&workflowservice.CountCallbackExecutionsRequest{Namespace: "ns"}))
		require.ErrorContains(t, rv.ValidateCountCallbackExecutions(&workflowservice.CountCallbackExecutionsRequest{}), "namespace is required")
	})
}
