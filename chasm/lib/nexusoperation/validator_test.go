package nexusoperation

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestValidateStartNexusOperationExecutionRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockVisibilityManager := manager.NewMockVisibilityManager(ctrl)
	mockVisibilityManager.EXPECT().GetIndexName().Return("index-name").AnyTimes()
	mockVisibilityManager.EXPECT().ValidateCustomSearchAttributes(gomock.Any()).Return(nil, nil).AnyTimes()

	saValidator := searchattribute.NewValidator(
		searchattribute.NewTestEsProvider(),
		searchattribute.NewTestMapperProvider(nil),
		func(string) int { return 2 },   // max number of keys
		func(string) int { return 20 },  // max size of value
		func(string) int { return 100 }, // max total size
		mockVisibilityManager,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
	)

	config := &Config{
		MaxIDLengthLimit:                   func() int { return 50 },
		MaxServiceNameLength:               func(string) int { return 10 },
		MaxOperationNameLength:             func(string) int { return 10 },
		PayloadSizeLimit:                   func(string) int { return 20 },
		PayloadSizeLimitWarn:               func(string) int { return 10 },
		MaxOperationHeaderSize:             func(string) int { return 10 },
		DisallowedOperationHeaders:         func() []string { return []string{"disallowed-header"} },
		MaxOperationScheduleToCloseTimeout: func(string) time.Duration { return time.Hour },
	}

	for _, tc := range []struct {
		name   string
		mutate func(*workflowservice.StartNexusOperationExecutionRequest)
		errMsg string
		check  func(*testing.T, *workflowservice.StartNexusOperationExecutionRequest)
	}{
		{
			name: "valid request",
		},
		{
			name: "operation_id - required",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.OperationId = ""
			},
			errMsg: "operation_id is required",
		},
		{
			name: "operation_id - exceeds length limit",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.OperationId = strings.Repeat("x", 51)
			},
			errMsg: "operation_id exceeds length limit",
		},
		{
			name: "request_id - defaults empty to UUID",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.RequestId = ""
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Len(t, r.RequestId, 36) // UUID length
			},
		},
		{
			name: "request_id - exceeds length limit",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.RequestId = strings.Repeat("x", 51)
			},
			errMsg: "request_id exceeds length limit",
		},
		{
			name: "identity - exceeds length limit",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.Identity = strings.Repeat("x", 51)
			},
			errMsg: "identity exceeds length limit",
		},
		{
			name:   "endpoint - required",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) { r.Endpoint = "" },
			errMsg: "endpoint is required",
		},
		{
			name:   "service - required",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) { r.Service = "" },
			errMsg: "service is required",
		},
		{
			name: "service - exceeds length limit",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.Service = "too-long-svc"
			},
			errMsg: "service exceeds length limit",
		},
		{
			name:   "operation - required",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) { r.Operation = "" },
			errMsg: "operation is required",
		},
		{
			name: "operation - exceeds length limit",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.Operation = "too-long-op!"
			},
			errMsg: "operation exceeds length limit",
		},
		{
			name: "schedule_to_close_timeout - invalid",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.ScheduleToCloseTimeout = &durationpb.Duration{Seconds: -1}
			},
			errMsg: "schedule_to_close_timeout is invalid",
		},
		{
			name: "schedule_to_close_timeout - caps exceeding max",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.ScheduleToCloseTimeout = durationpb.New(2 * time.Hour)
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, time.Hour, r.ScheduleToCloseTimeout.AsDuration())
			},
		},
		{
			name: "schedule_to_close_timeout - caps unset to max",
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, time.Hour, r.ScheduleToCloseTimeout.AsDuration())
			},
		},
		{
			name: "schedule_to_close_timeout - preserves within max",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.ScheduleToCloseTimeout = durationpb.New(30 * time.Minute)
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, 30*time.Minute, r.ScheduleToCloseTimeout.AsDuration())
			},
		},
		{
			name: "schedule_to_start_timeout - invalid",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.ScheduleToStartTimeout = &durationpb.Duration{Seconds: -1}
			},
			errMsg: "schedule_to_start_timeout is invalid",
		},
		{
			name: "schedule_to_start_timeout - caps to defaulted schedule_to_close_timeout",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.ScheduleToStartTimeout = durationpb.New(2 * time.Hour)
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, time.Hour, r.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, time.Hour, r.ScheduleToStartTimeout.AsDuration())
			},
		},
		{
			name: "schedule_to_start_timeout - caps to schedule_to_close_timeout",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.ScheduleToCloseTimeout = durationpb.New(30 * time.Minute)
				r.ScheduleToStartTimeout = durationpb.New(time.Hour)
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, 30*time.Minute, r.ScheduleToStartTimeout.AsDuration())
			},
		},
		{
			name: "schedule_to_start_timeout - preserves value within schedule_to_close_timeout",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.ScheduleToCloseTimeout = durationpb.New(30 * time.Minute)
				r.ScheduleToStartTimeout = durationpb.New(20 * time.Minute)
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, 20*time.Minute, r.ScheduleToStartTimeout.AsDuration())
			},
		},
		{
			name: "start_to_close_timeout - invalid",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.StartToCloseTimeout = &durationpb.Duration{Seconds: -1}
			},
			errMsg: "start_to_close_timeout is invalid",
		},
		{
			name: "start_to_close_timeout - caps to defaulted schedule_to_close_timeout",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.StartToCloseTimeout = durationpb.New(2 * time.Hour)
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, time.Hour, r.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, time.Hour, r.StartToCloseTimeout.AsDuration())
			},
		},
		{
			name: "start_to_close_timeout - caps to schedule_to_close_timeout",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.ScheduleToCloseTimeout = durationpb.New(30 * time.Minute)
				r.StartToCloseTimeout = durationpb.New(time.Hour)
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, 30*time.Minute, r.StartToCloseTimeout.AsDuration())
			},
		},
		{
			name: "start_to_close_timeout - preserves value within schedule_to_close_timeout",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.ScheduleToCloseTimeout = durationpb.New(30 * time.Minute)
				r.StartToCloseTimeout = durationpb.New(10 * time.Minute)
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, 10*time.Minute, r.StartToCloseTimeout.AsDuration())
			},
		},
		{
			name: "input - exceeds warning limit but within hard limit",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.Input = &commonpb.Payload{Data: []byte("exceed-warn-limit")}
			},
		},
		{
			name: "input - exceeds size limit",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.Input = &commonpb.Payload{Data: []byte("this-input-is-longer-than-twenty-characters")}
			},
			errMsg: "input exceeds size limit",
		},
		{
			name: "nexus_header - disallowed key",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.NexusHeader = map[string]string{"Disallowed-Header": "value"}
			},
			errMsg: "nexus_header contains a disallowed key",
		},
		{
			name: "nexus_header - exceeds size limit",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.NexusHeader = map[string]string{"key": "too-long-val"}
			},
			errMsg: "nexus_header exceeds size limit",
		},
		{
			name: "id_policies - defaults unspecified",
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE, r.IdReusePolicy)
				require.Equal(t, enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_FAIL, r.IdConflictPolicy)
			},
		},
		{
			name: "id_policies - preserves explicit values",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.IdReusePolicy = enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE
				r.IdConflictPolicy = enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING
			},
			check: func(t *testing.T, r *workflowservice.StartNexusOperationExecutionRequest) {
				require.Equal(t, enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE, r.IdReusePolicy)
				require.Equal(t, enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING, r.IdConflictPolicy)
			},
		},
		{
			name: "search_attributes - too many keys",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.SearchAttributes = &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"CustomKeywordField": payload.EncodeString("v1"),
						"CustomTextField":    payload.EncodeString("v2"),
						"CustomIntField":     payload.EncodeString("3"),
					},
				}
			},
			errMsg: "number of search attributes",
		},
		{
			name: "search_attributes - value exceeds size limit",
			mutate: func(r *workflowservice.StartNexusOperationExecutionRequest) {
				r.SearchAttributes = &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"CustomKeywordField": payload.EncodeString(strings.Repeat("x", 100)),
					},
				}
			},
			errMsg: "exceeds size limit",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &workflowservice.StartNexusOperationExecutionRequest{
				Namespace:   "default",
				OperationId: "op-id",
				RequestId:   "request-id",
				Endpoint:    "endpoint",
				Service:     "service",
				Operation:   "operation",
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"CustomKeywordField": payload.EncodeString("val"),
					},
				},
			}
			if tc.mutate != nil {
				tc.mutate(req)
			}
			err := validateAndNormalizeStartRequest(req, config, log.NewNoopLogger(), nil, saValidator)
			if tc.errMsg != "" {
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
			if tc.check != nil {
				tc.check(t, req)
			}
		})
	}
}

func TestValidateDescribeNexusOperationExecutionRequest(t *testing.T) {
	config := &Config{
		MaxIDLengthLimit: func() int { return 20 },
	}

	validRunID := "11111111-2222-3333-4444-555555555555"
	validToken, err := (&persistencespb.ChasmComponentRef{
		NamespaceId: "test-namespace-id",
		BusinessId:  "operation-id",
		RunId:       validRunID,
	}).Marshal()
	require.NoError(t, err)

	wrongNamespaceToken, err := (&persistencespb.ChasmComponentRef{
		NamespaceId: "other-namespace-id",
		BusinessId:  "operation-id",
		RunId:       validRunID,
	}).Marshal()
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		mutate func(*workflowservice.DescribeNexusOperationExecutionRequest)
		errMsg string
	}{
		{
			name: "valid request",
		},
		{
			name: "operation_id - required",
			mutate: func(r *workflowservice.DescribeNexusOperationExecutionRequest) {
				r.OperationId = ""
			},
			errMsg: "operation_id is required",
		},
		{
			name: "operation_id - exceeds length limit",
			mutate: func(r *workflowservice.DescribeNexusOperationExecutionRequest) {
				r.OperationId = "this-operation-id-is-too-long"
			},
			errMsg: "operation_id exceeds length limit",
		},
		{
			name: "run_id - not a valid UUID",
			mutate: func(r *workflowservice.DescribeNexusOperationExecutionRequest) {
				r.RunId = "not-a-uuid"
			},
			errMsg: "run_id is not a valid UUID",
		},
		{
			name: "long_poll_token - requires run_id",
			mutate: func(r *workflowservice.DescribeNexusOperationExecutionRequest) {
				r.LongPollToken = validToken
			},
			errMsg: "run_id is required when long_poll_token is provided",
		},
		{
			name: "long_poll_token - rejects malformed token",
			mutate: func(r *workflowservice.DescribeNexusOperationExecutionRequest) {
				r.RunId = validRunID
				r.LongPollToken = []byte("not-a-token")
			},
			errMsg: "invalid long poll token",
		},
		{
			name: "long_poll_token - rejects wrong namespace",
			mutate: func(r *workflowservice.DescribeNexusOperationExecutionRequest) {
				r.RunId = validRunID
				r.LongPollToken = wrongNamespaceToken
			},
			errMsg: "long poll token does not match execution",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validReq := &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   "default",
				OperationId: "operation-id",
			}
			if tc.mutate != nil {
				tc.mutate(validReq)
			}
			err := validateAndNormalizeDescribeRequest(validReq, "test-namespace-id", config)
			if tc.errMsg != "" {
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRequestCancelNexusOperationExecutionRequest(t *testing.T) {
	config := &Config{
		MaxIDLengthLimit: func() int { return 20 },
		MaxReasonLength:  func(string) int { return 20 },
	}

	for _, tc := range []struct {
		name   string
		mutate func(*workflowservice.RequestCancelNexusOperationExecutionRequest)
		errMsg string
		check  func(*testing.T, *workflowservice.RequestCancelNexusOperationExecutionRequest)
	}{
		{
			name: "valid request",
		},
		{
			name: "request_id - defaults empty to UUID",
			mutate: func(r *workflowservice.RequestCancelNexusOperationExecutionRequest) {
				r.RequestId = ""
			},
			check: func(t *testing.T, r *workflowservice.RequestCancelNexusOperationExecutionRequest) {
				require.Len(t, r.RequestId, 36)
			},
		},
		{
			name: "operation_id - required",
			mutate: func(r *workflowservice.RequestCancelNexusOperationExecutionRequest) {
				r.OperationId = ""
			},
			errMsg: "operation_id is required",
		},
		{
			name: "operation_id - exceeds length limit",
			mutate: func(r *workflowservice.RequestCancelNexusOperationExecutionRequest) {
				r.OperationId = "this-operation-id-is-too-long"
			},
			errMsg: "operation_id exceeds length limit",
		},
		{
			name: "request_id - exceeds length limit",
			mutate: func(r *workflowservice.RequestCancelNexusOperationExecutionRequest) {
				r.RequestId = "this-request-id-is-too-long"
			},
			errMsg: "request_id exceeds length limit",
		},
		{
			name: "run_id - not a valid UUID",
			mutate: func(r *workflowservice.RequestCancelNexusOperationExecutionRequest) {
				r.RunId = "not-a-uuid"
			},
			errMsg: "run_id is not a valid UUID",
		},
		{
			name: "identity - exceeds length limit",
			mutate: func(r *workflowservice.RequestCancelNexusOperationExecutionRequest) {
				r.Identity = "this-identity-is-too-long!!"
			},
			errMsg: "identity exceeds length limit",
		},
		{
			name: "reason - exceeds length limit",
			mutate: func(r *workflowservice.RequestCancelNexusOperationExecutionRequest) {
				r.Reason = "this-reason-is-longer-than-twenty-characters"
			},
			errMsg: "reason exceeds length limit",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validReq := &workflowservice.RequestCancelNexusOperationExecutionRequest{
				Namespace:   "default",
				OperationId: "operation-id",
			}
			if tc.mutate != nil {
				tc.mutate(validReq)
			}
			err := validateAndNormalizeCancelRequest(validReq, config)
			if tc.errMsg != "" {
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
			if tc.check != nil {
				tc.check(t, validReq)
			}
		})
	}
}

func TestValidateDeleteNexusOperationExecutionRequest(t *testing.T) {
	config := &Config{
		MaxIDLengthLimit: func() int { return 20 },
	}

	for _, tc := range []struct {
		name   string
		mutate func(*workflowservice.DeleteNexusOperationExecutionRequest)
		errMsg string
	}{
		{
			name: "valid request",
		},
		{
			name: "valid request - with run_id",
			mutate: func(r *workflowservice.DeleteNexusOperationExecutionRequest) {
				r.RunId = "550e8400-e29b-41d4-a716-446655440000"
			},
		},
		{
			name: "operation_id - required",
			mutate: func(r *workflowservice.DeleteNexusOperationExecutionRequest) {
				r.OperationId = ""
			},
			errMsg: "operation_id is required",
		},
		{
			name: "operation_id - exceeds length limit",
			mutate: func(r *workflowservice.DeleteNexusOperationExecutionRequest) {
				r.OperationId = "this-operation-id-is-too-long"
			},
			errMsg: "operation_id exceeds length limit",
		},
		{
			name: "run_id - invalid UUID",
			mutate: func(r *workflowservice.DeleteNexusOperationExecutionRequest) {
				r.RunId = "not-a-valid-uuid"
			},
			errMsg: "invalid run id: must be a valid UUID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validReq := &workflowservice.DeleteNexusOperationExecutionRequest{
				Namespace:   "default",
				OperationId: "operation-id",
			}
			if tc.mutate != nil {
				tc.mutate(validReq)
			}
			err := validateAndNormalizeDeleteRequest(validReq, config)
			if tc.errMsg != "" {
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTerminateNexusOperationExecutionRequest(t *testing.T) {
	config := &Config{
		MaxIDLengthLimit: func() int { return 20 },
		MaxReasonLength:  func(string) int { return 20 },
	}

	for _, tc := range []struct {
		name   string
		mutate func(*workflowservice.TerminateNexusOperationExecutionRequest)
		errMsg string
		check  func(*testing.T, *workflowservice.TerminateNexusOperationExecutionRequest)
	}{
		{
			name: "valid request",
		},
		{
			name: "request_id - defaults empty to UUID",
			mutate: func(r *workflowservice.TerminateNexusOperationExecutionRequest) {
				r.RequestId = ""
			},
			check: func(t *testing.T, r *workflowservice.TerminateNexusOperationExecutionRequest) {
				require.Len(t, r.RequestId, 36)
			},
		},
		{
			name: "operation_id - required",
			mutate: func(r *workflowservice.TerminateNexusOperationExecutionRequest) {
				r.OperationId = ""
			},
			errMsg: "operation_id is required",
		},
		{
			name: "operation_id - exceeds length limit",
			mutate: func(r *workflowservice.TerminateNexusOperationExecutionRequest) {
				r.OperationId = "this-operation-id-is-too-long"
			},
			errMsg: "operation_id exceeds length limit",
		},
		{
			name: "request_id - exceeds length limit",
			mutate: func(r *workflowservice.TerminateNexusOperationExecutionRequest) {
				r.RequestId = "this-request-id-is-too-long"
			},
			errMsg: "request_id exceeds length limit",
		},
		{
			name: "run_id - not a valid UUID",
			mutate: func(r *workflowservice.TerminateNexusOperationExecutionRequest) {
				r.RunId = "not-a-uuid"
			},
			errMsg: "run_id is not a valid UUID",
		},
		{
			name: "identity - exceeds length limit",
			mutate: func(r *workflowservice.TerminateNexusOperationExecutionRequest) {
				r.Identity = "this-identity-is-too-long!!"
			},
			errMsg: "identity exceeds length limit",
		},
		{
			name: "reason - exceeds length limit",
			mutate: func(r *workflowservice.TerminateNexusOperationExecutionRequest) {
				r.Reason = "this-reason-is-longer-than-twenty-characters"
			},
			errMsg: "reason exceeds length limit",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validReq := &workflowservice.TerminateNexusOperationExecutionRequest{
				Namespace:   "default",
				OperationId: "operation-id",
			}
			if tc.mutate != nil {
				tc.mutate(validReq)
			}
			err := validateAndNormalizeTerminateRequest(validReq, config)
			if tc.errMsg != "" {
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
			if tc.check != nil {
				tc.check(t, validReq)
			}
		})
	}
}

func TestValidatePollNexusOperationExecutionRequest(t *testing.T) {
	config := &Config{
		MaxIDLengthLimit: func() int { return 20 },
	}

	for _, tc := range []struct {
		name   string
		mutate func(*workflowservice.PollNexusOperationExecutionRequest)
		errMsg string
		check  func(*testing.T, *workflowservice.PollNexusOperationExecutionRequest)
	}{
		{
			name: "valid request",
		},
		{
			name: "operation_id - required",
			mutate: func(r *workflowservice.PollNexusOperationExecutionRequest) {
				r.OperationId = ""
			},
			errMsg: "operation_id is required",
		},
		{
			name: "operation_id - exceeds length limit",
			mutate: func(r *workflowservice.PollNexusOperationExecutionRequest) {
				r.OperationId = "this-operation-id-is-too-long"
			},
			errMsg: "operation_id exceeds length limit",
		},
		{
			name: "run_id - not a valid UUID",
			mutate: func(r *workflowservice.PollNexusOperationExecutionRequest) {
				r.RunId = "not-a-uuid"
			},
			errMsg: "run_id is not a valid UUID",
		},
		{
			name: "wait_stage - normalizes UNSPECIFIED to CLOSED",
			mutate: func(r *workflowservice.PollNexusOperationExecutionRequest) {
				r.WaitStage = enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED
			},
			check: func(t *testing.T, r *workflowservice.PollNexusOperationExecutionRequest) {
				require.Equal(t, enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED, r.WaitStage)
			},
		},
		{
			name: "wait_stage - preserves STARTED",
			mutate: func(r *workflowservice.PollNexusOperationExecutionRequest) {
				r.WaitStage = enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED
			},
			check: func(t *testing.T, r *workflowservice.PollNexusOperationExecutionRequest) {
				require.Equal(t, enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED, r.WaitStage)
			},
		},
		{
			name: "wait_stage - preserves CLOSED",
			mutate: func(r *workflowservice.PollNexusOperationExecutionRequest) {
				r.WaitStage = enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED
			},
			check: func(t *testing.T, r *workflowservice.PollNexusOperationExecutionRequest) {
				require.Equal(t, enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED, r.WaitStage)
			},
		},
		{
			name: "wait_stage - rejects unsupported value",
			mutate: func(r *workflowservice.PollNexusOperationExecutionRequest) {
				r.WaitStage = enumspb.NexusOperationWaitStage(99)
			},
			errMsg: "unsupported wait_stage",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validReq := &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   "default",
				OperationId: "operation-id",
			}
			if tc.mutate != nil {
				tc.mutate(validReq)
			}
			err := validateAndNormalizePollRequest(validReq, config)
			if tc.errMsg != "" {
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
			}
			if tc.check != nil {
				tc.check(t, validReq)
			}
		})
	}
}
