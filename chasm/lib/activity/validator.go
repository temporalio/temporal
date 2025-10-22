package activity

import (
	"fmt"

	"github.com/pborman/uuid"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
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

type ModifiedActivityRequestAttributes struct {
	ScheduleToStartTimeout *durationpb.Duration
	StartToCloseTimeout    *durationpb.Duration
	ScheduleToCloseTimeout *durationpb.Duration
	HeartbeatTimeout       *durationpb.Duration
}

type ModifiedStandaloneActivityRequestAttributes struct {
	requestID                 string
	searchAttributesUnaliased *commonpb.SearchAttributes // Unaliased search attributes after validation
}

// ValidateActivityRequestAttributes validates the common activity request attributes. This validation is shared by
// both standalone and embedded activities. It clones all timeout values from options before performing deductions to
// avoid modifying the original options parameter. This ensures the input remains unchanged during validation
// and all calculations are performed on local copies. Only after all deductions and validations
// succeed are the final computed values assigned to modifiedAttributes.
//
// The timeout normalization logic is as follows:
// 1. If ScheduleToClose is set, fill in missing ScheduleToStart and StartToClose from ScheduleToClose
// 2. If StartToClose is set but ScheduleToClose is not set, set ScheduleToClose to runTimeout, and fill in missing ScheduleToStart from runTimeout
// 3. If neither ScheduleToClose nor StartToClose is set, return error
// 4. Ensure all timeouts do not exceed runTimeout if runTimeout is set (>0)
// 5. Ensure HeartbeatTimeout does not exceed StartToClose
//
// Parameters:
//   - activityID: ID of the activity
//   - activityType: Type of the activity
//   - getDefaultActivityRetrySettings: Function to retrieve default retry settings for the namespace
//   - maxIDLengthLimit: Maximum allowed length for activity IDs and types
//   - namespaceID: ID of the namespace containing the activity
//   - options: Activity options including timeouts, task queue, and retry policy
//   - priority: Priority level for the activity task
//   - runTimeout: Workflow run timeout. Set to durationpb.New(0) if not applicable
//
// Returns the modified/normalized timeout attributes or an error if validation fails.
func ValidateActivityRequestAttributes(
	activityID string,
	activityType string,
	getDefaultActivityRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings],
	maxIDLengthLimit int,
	namespaceID namespace.ID,
	options *activitypb.ActivityOptions,
	priority *commonpb.Priority,
	runTimeout *durationpb.Duration) (*ModifiedActivityRequestAttributes, error) {
	if err := tqid.NormalizeAndValidate(options.TaskQueue, "", maxIDLengthLimit); err != nil {
		return nil, fmt.Errorf("invalid TaskQueue: %w. ActivityId=%s ActivityType=%s", err, activityID, activityType)
	}

	if activityID == "" {
		return nil, serviceerror.NewInvalidArgumentf("ActivityId is not set. ActivityType=%s", activityType)
	}
	if activityType == "" {
		return nil, serviceerror.NewInvalidArgumentf("ActivityType is not set. ActivityID=%s", activityID)
	}

	if err := validateActivityRetryPolicy(namespaceID, options.RetryPolicy, getDefaultActivityRetrySettings); err != nil {
		return nil, fmt.Errorf("invalid ActivityRetryPolicy: %w. ActivityId=%s ActivityType=%s", err, activityID, activityType)
	}

	if len(activityID) > maxIDLengthLimit {
		return nil, serviceerror.NewInvalidArgumentf("ActivityId exceeds length limit. ActivityId=%s ActivityType=%s Length=%d Limit=%d",
			activityID, activityType, len(activityID), maxIDLengthLimit)
	}
	if len(activityType) > maxIDLengthLimit {
		return nil, serviceerror.NewInvalidArgumentf("ActivityType exceeds length limit. ActivityId=%s ActivityType=%s Length=%d Limit=%d",
			activityID, activityType, len(activityType), maxIDLengthLimit)
	}

	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if err := timestamp.ValidateAndCapProtoDuration(options.GetScheduleToCloseTimeout()); err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid ScheduleToCloseTimeout: % ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}
	if err := timestamp.ValidateAndCapProtoDuration(options.GetScheduleToStartTimeout()); err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid ScheduleToStartTimeout: % ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}
	if err := timestamp.ValidateAndCapProtoDuration(options.GetStartToCloseTimeout()); err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid StartToCloseTimeout: % ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}
	if err := timestamp.ValidateAndCapProtoDuration(options.GetHeartbeatTimeout()); err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid HeartbeatTimeout: % ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}

	if err := priorities.Validate(priority); err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid Priorities: % ActivityId=%s ActivityType=%s",
			err, activityID, activityType)
	}

	modifiedAttributes := &ModifiedActivityRequestAttributes{}

	if err := populateTimeouts(activityID,
		activityType,
		runTimeout,
		options,
		modifiedAttributes); err != nil {
		return nil, err
	}

	return modifiedAttributes, nil
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

func populateTimeouts(
	activityID string,
	activityType string,
	runTimeout *durationpb.Duration,
	options *activitypb.ActivityOptions,
	modifiedAttributes *ModifiedActivityRequestAttributes,
) error {
	ScheduleToCloseSet := options.GetScheduleToCloseTimeout().AsDuration() > 0
	ScheduleToStartSet := options.GetScheduleToStartTimeout().AsDuration() > 0
	StartToCloseSet := options.GetStartToCloseTimeout().AsDuration() > 0

	// The logic to set the timeouts is "cumulative", so we must clone to local variables to avoid modifying the
	// original options until all deductions are done.
	currentScheduleToClose := common.CloneProto(options.GetScheduleToCloseTimeout())
	currentStartToClose := common.CloneProto(options.GetStartToCloseTimeout())
	currentScheduleToStart := common.CloneProto(options.GetScheduleToStartTimeout())
	currentHeartbeat := common.CloneProto(options.GetHeartbeatTimeout())

	if ScheduleToCloseSet {
		if ScheduleToStartSet {
			currentScheduleToStart = timestamp.MinDurationPtr(currentScheduleToStart, currentScheduleToClose)
		} else {
			currentScheduleToStart = currentScheduleToClose
		}
		if StartToCloseSet {
			currentStartToClose = timestamp.MinDurationPtr(currentStartToClose, currentScheduleToClose)
		} else {
			currentStartToClose = currentScheduleToClose
		}
	} else if StartToCloseSet {
		// We are in !validScheduleToClose due to the first if above
		currentScheduleToClose = runTimeout
		if !ScheduleToStartSet {
			currentScheduleToStart = runTimeout
		}
	} else {
		// Deduction failed as there's not enough information to fill in missing timeouts.
		return serviceerror.NewInvalidArgumentf("A valid StartToClose or ScheduleToCloseTimeout is not set on ScheduleActivityTaskCommand. ActivityId=%s ActivityType=%s",
			activityID, activityType)
	}
	// ensure activity timeout never larger than workflow timeout
	if runTimeout.AsDuration() > 0 {
		runTimeoutDur := runTimeout.AsDuration()
		if currentScheduleToClose.AsDuration() > runTimeoutDur {
			currentScheduleToClose = runTimeout
		}
		if currentScheduleToStart.AsDuration() > runTimeoutDur {
			currentScheduleToStart = runTimeout
		}
		if currentStartToClose.AsDuration() > runTimeoutDur {
			currentStartToClose = runTimeout
		}
		if currentHeartbeat.AsDuration() > runTimeoutDur {
			currentHeartbeat = runTimeout
		}
	}

	currentHeartbeat = timestamp.MinDurationPtr(currentHeartbeat, currentStartToClose)

	modifiedAttributes.ScheduleToCloseTimeout = currentScheduleToClose
	modifiedAttributes.ScheduleToStartTimeout = currentScheduleToStart
	modifiedAttributes.StartToCloseTimeout = currentStartToClose
	modifiedAttributes.HeartbeatTimeout = currentHeartbeat

	return nil
}

// ValidateStandaloneActivity validates the standalone activity specific attributes. It will return the normalized/modified
// attributes, letting the caller decide how to handle them.
//
// Parameters:
//   - activityID: Unique identifier for the activity
//   - activityType: Type/name of the activity being scheduled
//   - blobSizeLimitError: Function to retrieve the error threshold for input size per namespace
//   - blobSizeLimitWarn: Function to retrieve the warning threshold for input size per namespace
//   - inputSizeBytes: Size of the activity input in bytes
//   - logger: Logger instance for recording warnings and errors
//   - maxIDLengthLimit: Maximum allowed length for activity IDs and request IDs
//   - namespaceName: Name of the namespace containing the activity
//   - requestID: Client-provided request ID for idempotency (auto-generated if empty)
//   - searchAttributes: Search attributes to validate and unalias
//   - saMapperProvider: Provider for mapping aliased search attribute names
//   - saValidator: Validator for search attributes
//
// Returns the modified attributes (request ID and unaliased search attributes) if they have been sanitized or an error
// if validation fails.
func ValidateStandaloneActivity(
	activityID string,
	activityType string,
	blobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter,
	blobSizeLimitWarn dynamicconfig.IntPropertyFnWithNamespaceFilter,
	inputSizeBytes int,
	logger log.Logger,
	maxIDLengthLimit int,
	namespaceName string,
	requestID string,
	searchAttributes *commonpb.SearchAttributes,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator) (*ModifiedStandaloneActivityRequestAttributes, error) {
	modifiedAttributes := &ModifiedStandaloneActivityRequestAttributes{}

	if err := validateRequestID(requestID, maxIDLengthLimit, modifiedAttributes); err != nil {
		return nil, err
	}

	if err := validateInputSize(
		activityID,
		activityType,
		blobSizeLimitError,
		blobSizeLimitWarn,
		inputSizeBytes,
		logger,
		namespaceName); err != nil {
		return nil, err
	}

	if searchAttributes != nil {
		if err := validateAndUnaliasSearchAttributes(
			modifiedAttributes,
			namespaceName,
			searchAttributes,
			saMapperProvider,
			saValidator); err != nil {
			return nil, err
		}
	}

	return modifiedAttributes, nil
}

func validateRequestID(requestID string, maxIDLengthLimit int, modifiedAttributes *ModifiedStandaloneActivityRequestAttributes) error {
	if requestID == "" {
		// For easy direct API use, we default the request ID here but expect all SDKs and other auto-retrying clients to set it
		modifiedAttributes.requestID = uuid.New()
	}

	if len(requestID) > maxIDLengthLimit || len(modifiedAttributes.requestID) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("RequestID length exceeds limit.")
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
	namespaceName string) error {
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

func validateAndUnaliasSearchAttributes(
	modifiedAttributes *ModifiedStandaloneActivityRequestAttributes,
	namespaceName string,
	searchAttributes *commonpb.SearchAttributes,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator) error {
	unaliasedSaa, err := searchattribute.UnaliasFields(saMapperProvider, searchAttributes, namespaceName)
	if err != nil {
		return err
	}

	if err := saValidator.Validate(unaliasedSaa, namespaceName); err != nil {
		return err
	}

	if err := saValidator.ValidateSize(unaliasedSaa, namespaceName); err != nil {
		return err
	}

	modifiedAttributes.searchAttributesUnaliased = unaliasedSaa

	return nil
}
