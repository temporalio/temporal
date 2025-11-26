package activity

import (
	"fmt"

	"github.com/google/uuid"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/priorities"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ValidateAndNormalizeActivityAttributes validates and normalizes the common activity request attributes.
// This validation is shared by both standalone and embedded activities.
// IMPORTANT: this method mutates the input params; in cases where it's critical to maintain immutability
// (i.e., when incoming request can potentially be retried), clone the params first before passing it in.
//
// The timeout normalization logic is as follows:
// 1. If ScheduleToClose is set, fill in missing ScheduleToStart and StartToClose from ScheduleToClose
// 2. If StartToClose is set but ScheduleToClose is not set, set ScheduleToClose to runTimeout, and fill in missing ScheduleToStart from runTimeout
// 3. If neither ScheduleToClose nor StartToClose is set, return error
// 4. Ensure all timeouts do not exceed runTimeout if runTimeout is set (>0)
// 5. Ensure HeartbeatTimeout does not exceed StartToClose
func ValidateAndNormalizeActivityAttributes(
	activityID string,
	activityType string,
	getDefaultActivityRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings],
	maxIDLengthLimit int,
	namespaceID namespace.ID,
	options *activitypb.ActivityOptions,
	priority *commonpb.Priority,
	runTimeout *durationpb.Duration,
) error {
	if err := tqid.NormalizeAndValidate(options.TaskQueue, "", maxIDLengthLimit); err != nil {
		return fmt.Errorf("invalid TaskQueue: %w. ActivityId=%s ActivityType=%s", err, activityID, activityType)
	}

	if activityID == "" {
		return serviceerror.NewInvalidArgumentf("ActivityId is not set. ActivityType=%s", activityType)
	}
	if activityType == "" {
		return serviceerror.NewInvalidArgumentf("ActivityType is not set. ActivityID=%s", activityID)
	}

	if err := validateActivityRetryPolicy(namespaceID, options.RetryPolicy, getDefaultActivityRetrySettings); err != nil {
		return fmt.Errorf("invalid ActivityRetryPolicy: %w. ActivityId=%s ActivityType=%s", err, activityID, activityType)
	}

	if len(activityID) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("ActivityId exceeds length limit. ActivityId=%s ActivityType=%s Length=%d Limit=%d",
			activityID, activityType, len(activityID), maxIDLengthLimit)
	}
	if len(activityType) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("ActivityType exceeds length limit. ActivityId=%s ActivityType=%s Length=%d Limit=%d",
			activityID, activityType, len(activityType), maxIDLengthLimit)
	}

	if err := priorities.Validate(priority); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid Priorities: %v ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}

	return normalizeAndValidateTimeouts(activityID,
		activityType,
		runTimeout,
		options)
}

func validateActivityRetryPolicy(
	namespaceID namespace.ID,
	retryPolicy *commonpb.RetryPolicy,
	getDefaultActivityRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings],
) error {
	if retryPolicy == nil {
		return nil
	}
	// TODO: this is a namespace setting, not a namespace id setting
	defaultActivityRetrySettings := getDefaultActivityRetrySettings(namespaceID.String())
	retrypolicy.EnsureDefaults(retryPolicy, defaultActivityRetrySettings)
	return retrypolicy.Validate(retryPolicy)
}

func normalizeAndValidateTimeouts(
	activityID string,
	activityType string,
	runTimeout *durationpb.Duration,
	options *activitypb.ActivityOptions,
) error {
	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if err := timestamp.ValidateAndCapProtoDuration(options.GetScheduleToCloseTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid ScheduleToCloseTimeout: %v ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}
	if err := timestamp.ValidateAndCapProtoDuration(options.GetScheduleToStartTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid ScheduleToStartTimeout: %v ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}
	if err := timestamp.ValidateAndCapProtoDuration(options.GetStartToCloseTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid StartToCloseTimeout: %v ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}
	if err := timestamp.ValidateAndCapProtoDuration(options.GetHeartbeatTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid HeartbeatTimeout: %v ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}

	scheduleToCloseSet := options.GetScheduleToCloseTimeout().AsDuration() > 0
	scheduleToStartSet := options.GetScheduleToStartTimeout().AsDuration() > 0
	startToCloseSet := options.GetStartToCloseTimeout().AsDuration() > 0

	if scheduleToCloseSet {
		if scheduleToStartSet {
			options.ScheduleToStartTimeout = timestamp.MinDurationPtr(options.ScheduleToStartTimeout, options.ScheduleToCloseTimeout)
		} else {
			options.ScheduleToStartTimeout = options.ScheduleToCloseTimeout
		}
		if startToCloseSet {
			options.StartToCloseTimeout = timestamp.MinDurationPtr(options.StartToCloseTimeout, options.ScheduleToCloseTimeout)
		} else {
			options.StartToCloseTimeout = options.ScheduleToCloseTimeout
		}
	} else if startToCloseSet {
		// We are in !validScheduleToClose due to the first if above
		options.ScheduleToCloseTimeout = runTimeout
		if !scheduleToStartSet {
			options.ScheduleToStartTimeout = runTimeout
		}
	} else {
		// Deduction failed as there's not enough information to fill in missing timeouts.
		return serviceerror.NewInvalidArgumentf("A valid StartToClose or ScheduleToCloseTimeout is not set on ScheduleActivityTaskCommand. ActivityId=%s ActivityType=%s",
			activityID, activityType)
	}
	// ensure activity timeout never larger than workflow timeout
	if runTimeout.AsDuration() > 0 {
		runTimeoutDur := runTimeout.AsDuration()
		if options.ScheduleToCloseTimeout.AsDuration() > runTimeoutDur {
			options.ScheduleToCloseTimeout = runTimeout
		}
		if options.ScheduleToStartTimeout.AsDuration() > runTimeoutDur {
			options.ScheduleToStartTimeout = runTimeout
		}
		if options.StartToCloseTimeout.AsDuration() > runTimeoutDur {
			options.StartToCloseTimeout = runTimeout
		}
		if options.HeartbeatTimeout.AsDuration() > runTimeoutDur {
			options.HeartbeatTimeout = runTimeout
		}
	}

	options.HeartbeatTimeout = timestamp.MinDurationPtr(options.HeartbeatTimeout, options.StartToCloseTimeout)

	return nil
}

// ValidateStandaloneActivity validates and normalizes the standalone activity specific attributes.
// IMPORTANT: this method mutates the input params; in cases where it's critical to maintain immutability
// (i.e., when incoming request can potentially be retried), clone the params first before passing it in.
func ValidateStandaloneActivity(
	req *workflowservice.StartActivityExecutionRequest,
	blobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter,
	blobSizeLimitWarn dynamicconfig.IntPropertyFnWithNamespaceFilter,
	logger log.Logger,
	maxIDLengthLimit int,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) error {
	if err := validateAndNormalizeRequestID(&req.RequestId, maxIDLengthLimit); err != nil {
		return err
	}

	if err := normalizeAndValidateIDPolicy(req); err != nil {
		return err
	}

	if err := validateInputSize(
		req.GetActivityId(),
		req.GetActivityType().GetName(),
		blobSizeLimitError,
		blobSizeLimitWarn,
		req.Input.Size(),
		logger,
		req.GetNamespace()); err != nil {
		return err
	}

	if req.GetSearchAttributes() != nil {
		if err := validateAndNormalizeSearchAttributes(
			req,
			saMapperProvider,
			saValidator); err != nil {
			return err
		}
	}

	return nil
}

func validateAndNormalizeRequestID(requestID *string, maxIDLengthLimit int) error {
	if *requestID == "" {
		// For easy direct API use, we default the request ID here but expect all SDKs and other auto-retrying clients to set it
		*requestID = uuid.New().String()
	}

	if len(*requestID) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("RequestID length exceeds limit.")
	}

	return nil
}

func normalizeAndValidateIDPolicy(req *workflowservice.StartActivityExecutionRequest) error {
	if req.GetIdConflictPolicy() == enumspb.ACTIVITY_ID_CONFLICT_POLICY_TERMINATE_EXISTING &&
		req.GetIdReusePolicy() == enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE {
		return serviceerror.NewInvalidArgument("Invalid ActivityIdReusePolicy: ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE " +
			"cannot be used together with ActivityIdConflictPolicy ACTIVITY_ID_CONFLICT_POLICY_TERMINATE_EXISTING")
	}

	if req.GetIdReusePolicy() == enumspb.ACTIVITY_ID_REUSE_POLICY_UNSPECIFIED {
		req.IdReusePolicy = enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}

	if req.GetIdConflictPolicy() == enumspb.ACTIVITY_ID_CONFLICT_POLICY_UNSPECIFIED {
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL
	}

	if req.GetOnConflictOptions().GetAttachCompletionCallbacks() && !req.GetOnConflictOptions().GetAttachRequestId() {
		return serviceerror.NewInvalidArgument("attaching request ID is required for attaching completion callbacks")
	}

	return nil
}

func validateInputSize(
	activityID string,
	activityType string,
	blobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter,
	blobSizeLimitWarn dynamicconfig.IntPropertyFnWithNamespaceFilter,
	inputSize int,
	logger log.Logger,
	namespaceName string,
) error {
	sizeWarnLimit := blobSizeLimitWarn(namespaceName)
	sizeErrorLimit := blobSizeLimitError(namespaceName)

	if inputSize > sizeWarnLimit {
		logger.Warn("Activity input size exceeds the warning limit.",
			tag.WorkflowNamespace(namespaceName),
			tag.ActivityID(activityID),
			tag.ActivitySize(int64(inputSize)),
			tag.BlobSizeViolationOperation(activityType))

		if inputSize > sizeErrorLimit {
			return common.ErrBlobSizeExceedsLimit
		}
	}

	return nil
}

func validateAndNormalizeSearchAttributes(
	req *workflowservice.StartActivityExecutionRequest,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) error {
	namespaceName := req.GetNamespace()

	unaliased, err := searchattribute.UnaliasFields(saMapperProvider, req.SearchAttributes, namespaceName)
	if err != nil {
		return err
	}

	req.SearchAttributes.IndexedFields = unaliased.IndexedFields

	if err := saValidator.Validate(req.SearchAttributes, namespaceName); err != nil {
		return err
	}

	return saValidator.ValidateSize(req.SearchAttributes, namespaceName)
}

// ValidatePollActivityExecutionRequest validates the request for PollActivityExecution API.
func ValidatePollActivityExecutionRequest(
	req *workflowservice.PollActivityExecutionRequest,
	maxIDLengthLimit int,
) error {
	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}
	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}
	if req.GetRunId() == "" {
		return serviceerror.NewInvalidArgument("run id is required")
	}
	if _, err := uuid.Parse(req.GetRunId()); err != nil {
		return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
	}
	return nil
}
