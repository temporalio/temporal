package activityoptions

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestMergeActivityOptionsAcceptance(t *testing.T) {
	updateOptions := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		Priority: &commonpb.Priority{
			PriorityKey:    42,
			FairnessKey:    "test_key",
			FairnessWeight: 5.0,
		},
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
		},
	}

	testCases := []struct {
		name      string
		mergeInto *activitypb.ActivityOptions
		mergeFrom *activitypb.ActivityOptions
		expected  *activitypb.ActivityOptions
		mask      *fieldmaskpb.FieldMask
	}{
		{
			name:      "Top-level fields with CamelCase",
			mergeFrom: updateOptions,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  updateOptions,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
					"ScheduleToCloseTimeout",
					"ScheduleToStartTimeout",
					"StartToCloseTimeout",
					"HeartbeatTimeout",
					"Priority",
					"RetryPolicy",
				},
			},
		},
		{
			name:      "Top-level fields with snake_case",
			mergeFrom: updateOptions,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  updateOptions,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"task_queue.name",
					"schedule_to_close_timeout",
					"schedule_to_start_timeout",
					"start_to_close_timeout",
					"heartbeat_timeout",
					"priority",
					"retry_policy",
				},
			},
		},
		{
			name: "Sub-fields",
			mergeFrom: &activitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				},
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			mergeInto: &activitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    10,
					FairnessKey:    "oldKey",
					FairnessWeight: 1.0,
				},
				RetryPolicy: &commonpb.RetryPolicy{},
			},
			expected: &activitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				},
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"priority.priority_key",
					"priority.fairness_key",
					"priority.fairness_weight",
					"retry_policy.backoff_coefficient",
					"retry_policy.initial_interval",
					"retry_policy.maximum_interval",
					"retry_policy.maximum_attempts",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updateFields := util.ParseFieldMask(tc.mask)
			err := MergeActivityOptions(tc.mergeInto, tc.mergeFrom, updateFields)
			require.NoError(t, err)
			require.Equal(t, tc.expected.RetryPolicy.GetInitialInterval(), tc.mergeInto.RetryPolicy.GetInitialInterval(), "RetryInitialInterval")
			require.Equal(t, tc.expected.RetryPolicy.GetMaximumInterval(), tc.mergeInto.RetryPolicy.GetMaximumInterval(), "RetryMaximumInterval")
			require.InEpsilon(t, tc.expected.RetryPolicy.GetBackoffCoefficient(), tc.mergeInto.RetryPolicy.GetBackoffCoefficient(), 0.001, "RetryBackoffCoefficient")
			require.Equal(t, tc.expected.RetryPolicy.GetMaximumAttempts(), tc.mergeInto.RetryPolicy.GetMaximumAttempts(), "RetryMaximumAttempts")
			require.Equal(t, tc.expected.TaskQueue, tc.mergeInto.TaskQueue, "TaskQueue")
			require.Equal(t, tc.expected.ScheduleToCloseTimeout, tc.mergeInto.ScheduleToCloseTimeout, "ScheduleToCloseTimeout")
			require.Equal(t, tc.expected.ScheduleToStartTimeout, tc.mergeInto.ScheduleToStartTimeout, "ScheduleToStartTimeout")
			require.Equal(t, tc.expected.StartToCloseTimeout, tc.mergeInto.StartToCloseTimeout, "StartToCloseTimeout")
			require.Equal(t, tc.expected.HeartbeatTimeout, tc.mergeInto.HeartbeatTimeout, "HeartbeatTimeout")
			require.Equal(t, tc.expected.Priority, tc.mergeInto.Priority, "Priority")
		})
	}
}

func TestMergeActivityOptionsErrors(t *testing.T) {
	makeReq := func(paths ...string) map[string]struct{} {
		return util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: paths})
	}
	emptyOpts := &activitypb.ActivityOptions{}

	var err error
	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("retry_policy.maximum_interval"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("retry_policy.maximum_attempts"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("retry_policy.backoff_coefficient"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("retry_policy.initial_interval"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("taskQueue.name"))
	require.ErrorContains(t, err, "TaskQueue is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("priority.priority_key"))
	require.ErrorContains(t, err, "Priority is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("priority.fairness_key"))
	require.ErrorContains(t, err, "Priority is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("priority.fairness_weight"))
	require.ErrorContains(t, err, "Priority is not provided")
}

// TestMergeActivityOptionsCoversAllFields uses protobuf reflection to enumerate
// every field-mask path reachable from ActivityOptions and verifies that each
// one is handled by MergeActivityOptions. Paths that are intentionally not
// offered for update must be listed in notOfferedPaths. When a new field is
// added to the proto, this test will automatically create a sub-test that fails
// until MergeActivityOptions handles it or it is added to notOfferedPaths.
func TestMergeActivityOptionsCoversAllFields(t *testing.T) {
	// notOfferedPaths lists camelCase field-mask paths intentionally not supported
	// for update. Add a new entry here (with a comment) if a proto field should
	// not be offered to callers rather than wiring it into MergeActivityOptions.
	notOfferedPaths := map[string]struct{}{
		// TaskQueue: only the name is user-writable; kind and normal_name are
		// internal routing fields managed by the server, not by callers.
		"taskQueue":            {},
		"taskQueue.kind":       {},
		"taskQueue.normalName": {},
		// non_retryable_error_types is a repeated field; set/clear semantics
		// do not map cleanly onto a single field-mask path (no append/remove).
		"retryPolicy.nonRetryableErrorTypes": {},
	}

	// Enumerate all valid field-mask paths for ActivityOptions via protoreflect.
	discovered := collectActivityOptionsFieldPaths()

	// notOfferedPaths entries must still exist in the proto (catches stale entries after renames).
	discoveredSet := make(map[string]struct{}, len(discovered))
	for _, p := range discovered {
		discoveredSet[p] = struct{}{}
	}
	for p := range notOfferedPaths {
		_, exists := discoveredSet[p]
		require.True(t, exists, "notOfferedPaths entry %q does not correspond to any proto field path", p)
	}

	// populatedOptions has a distinct non-zero value for every possible field so
	// the per-path mutation check below can detect whether the field was actually set.
	populatedOptions := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-queue"},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		StartToCloseTimeout:    durationpb.New(3 * time.Second),
		HeartbeatTimeout:       durationpb.New(1 * time.Second),
		Priority: &commonpb.Priority{
			PriorityKey:    42,
			FairnessKey:    "test-key",
			FairnessWeight: 2.5,
		},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 2.0,
			MaximumInterval:    durationpb.New(30 * time.Second),
			MaximumAttempts:    5,
		},
	}

	// Every discovered path not in notOfferedPaths must be handled by MergeActivityOptions.
	for _, path := range discovered {
		if _, skip := notOfferedPaths[path]; skip {
			continue
		}
		path := path // capture for sub-test closure
		t.Run("updates/"+path, func(t *testing.T) {
			mergeInto := &activitypb.ActivityOptions{}
			err := MergeActivityOptions(mergeInto, populatedOptions, map[string]struct{}{path: {}})
			require.NoError(t, err, "unexpected error for path %q", path)

			fromVal, fd := getValueAtProtoPath(populatedOptions.ProtoReflect(), path)
			intoVal, _ := getValueAtProtoPath(mergeInto.ProtoReflect(), path)

			switch fd.Kind() {
			case protoreflect.MessageKind:
				require.True(t, proto.Equal(safeProtoMessage(fromVal), safeProtoMessage(intoVal)),
					"MergeActivityOptions did not update message field %q", path)
			default:
				require.Equal(t, fromVal.Interface(), intoVal.Interface(),
					"MergeActivityOptions did not update scalar field %q", path)
			}
		})
	}
}

// collectActivityOptionsFieldPaths returns the camelCase field-mask paths for
// ActivityOptions. For singular message-typed fields, both the message path and
// its sub-field paths are included. Well-known types (e.g. google.protobuf.Duration)
// are treated as leaves and not recursed into.
func collectActivityOptionsFieldPaths() []string {
	return collectFieldMaskPaths((&activitypb.ActivityOptions{}).ProtoReflect().Descriptor(), "")
}

func collectFieldMaskPaths(desc protoreflect.MessageDescriptor, prefix string) []string {
	var paths []string
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fullPath := field.JSONName()
		if prefix != "" {
			fullPath = prefix + "." + field.JSONName()
		}
		paths = append(paths, fullPath)

		// Recurse into singular message fields that are not google.protobuf well-known types.
		if field.Kind() == protoreflect.MessageKind &&
			!field.IsList() && !field.IsMap() &&
			!isWellKnownProtoMessage(field.Message()) {
			paths = append(paths, collectFieldMaskPaths(field.Message(), fullPath)...)
		}
	}
	return paths
}

// isWellKnownProtoMessage reports whether msg is a google.protobuf well-known
// type such as Duration or Timestamp. Field-mask paths never descend into these.
func isWellKnownProtoMessage(msg protoreflect.MessageDescriptor) bool {
	return strings.HasPrefix(string(msg.FullName()), "google.protobuf.")
}

// getValueAtProtoPath navigates a dot-separated camelCase field-mask path in msg
// and returns the protoreflect.Value and FieldDescriptor at that path.
// Returns an invalid Value if any intermediate message is not populated.
func getValueAtProtoPath(msg protoreflect.Message, path string) (protoreflect.Value, protoreflect.FieldDescriptor) {
	parts := strings.SplitN(path, ".", 2)
	fd := msg.Descriptor().Fields().ByJSONName(parts[0])
	if fd == nil {
		panic("proto field not found by JSON name: " + parts[0])
	}
	val := msg.Get(fd)
	if len(parts) == 1 {
		return val, fd
	}
	if fd.Kind() != protoreflect.MessageKind || !val.Message().IsValid() {
		return protoreflect.Value{}, fd
	}
	return getValueAtProtoPath(val.Message(), parts[1])
}

// safeProtoMessage returns the proto.Message from a protoreflect.Value,
// or nil if the value is invalid or the underlying message is not populated.
func safeProtoMessage(v protoreflect.Value) proto.Message {
	if !v.IsValid() || !v.Message().IsValid() {
		return nil
	}
	return v.Message().Interface()
}
