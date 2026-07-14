package validate_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/protohelpers/validate"
)

// startRequest is a representative production validator: a handful of required
// fields, everything else optional. Declaring it exercises the exhaustive
// struct — every field must be assigned a rule or the code below fails the
// exhaustiveness test.
var startRequest = validate.StartWorkflowExecutionRequest{
	Namespace:                    validate.Required(),
	WorkflowId:                   validate.Required(),
	WorkflowType:                 validate.Required(),
	TaskQueue:                    validate.Required(),
	RequestId:                    validate.Required(),
	Input:                        validate.Optional(),
	WorkflowExecutionTimeout:     validate.Optional(),
	WorkflowRunTimeout:           validate.Optional(),
	WorkflowTaskTimeout:          validate.Optional(),
	Identity:                     validate.Optional(),
	WorkflowIdReusePolicy:        validate.Optional(),
	WorkflowIdConflictPolicy:     validate.Optional(),
	RetryPolicy:                  validate.Optional(),
	CronSchedule:                 validate.Optional(),
	Memo:                         validate.Optional(),
	SearchAttributes:             validate.Optional(),
	Header:                       validate.Optional(),
	RequestEagerExecution:        validate.Optional(),
	ContinuedFailure:             validate.Optional(),
	LastCompletionResult:         validate.Optional(),
	WorkflowStartDelay:           validate.Optional(),
	CompletionCallbacks:          validate.Optional(),
	UserMetadata:                 validate.Optional(),
	Links:                        validate.Optional(),
	VersioningOverride:           validate.Optional(),
	OnConflictOptions:            validate.Optional(),
	Priority:                     validate.Optional(),
	EagerWorkerDeploymentOptions: validate.Optional(),
	TimeSkippingConfig:           validate.Optional(),
}

func validRequest() *workflowservice.StartWorkflowExecutionRequest {
	return &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    "ns",
		WorkflowId:   "wf-1",
		WorkflowType: &commonpb.WorkflowType{Name: "T"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "tq"},
		RequestId:    "req-1",
	}
}

func TestValidate_Valid(t *testing.T) {
	require.NoError(t, startRequest.Validate(validRequest()))
}

func TestValidate_MissingRequired(t *testing.T) {
	req := validRequest()
	req.WorkflowId = ""
	req.TaskQueue = nil

	err := startRequest.Validate(req)
	require.Error(t, err)
	require.ErrorContains(t, err, "workflow_id: expected a non-empty value")
	require.ErrorContains(t, err, "task_queue: expected a non-empty value")
	require.NotContains(t, err.Error(), "namespace", "set fields must not be flagged")
}

// TestValidate_Exhaustive proves startRequest assigns a rule to every field: a
// missing rule would surface as a "no rule specified" violation. This is the
// production analogue of the matcher's exhaustiveness guard.
func TestValidate_Exhaustive(t *testing.T) {
	err := startRequest.Validate(&workflowservice.StartWorkflowExecutionRequest{})
	require.Error(t, err) // required fields are unset
	require.NotContains(t, err.Error(), "no rule specified")
}

func TestValidate_MissingRuleIsCaught(t *testing.T) {
	// A validator that forgets fields must fail loudly rather than silently
	// pass them.
	incomplete := validate.StartWorkflowExecutionRequest{Namespace: validate.Required()}
	err := incomplete.Validate(validRequest())
	require.Error(t, err)
	require.ErrorContains(t, err, "workflow_id: no rule specified")
}

func TestValidate_Oneof(t *testing.T) {
	v := validate.Command{
		CommandType:       validate.Required(),
		Attributes:        validate.Required(), // a oneof member must be set
		UserMetadata:      validate.Optional(),
		EventGroupMarkers: validate.Optional(),
	}

	// Missing oneof member.
	err := v.Validate(&commandpb.Command{CommandType: enumspb.COMMAND_TYPE_START_TIMER})
	require.ErrorContains(t, err, "attributes: expected a non-empty value")

	// Oneof member set.
	ok := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{
			StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{TimerId: "t"},
		},
	}
	require.NoError(t, v.Validate(ok))
}

func TestValidate_Custom(t *testing.T) {
	v := validate.Memo{
		Fields: validate.Custom(func(got any) error {
			if len(got.(map[string]*commonpb.Payload)) > 3 {
				return errors.New("too many memo fields")
			}
			return nil
		}),
	}
	require.NoError(t, v.Validate(&commonpb.Memo{Fields: map[string]*commonpb.Payload{"a": {}}}))

	big := &commonpb.Memo{Fields: map[string]*commonpb.Payload{"a": {}, "b": {}, "c": {}, "d": {}}}
	require.ErrorContains(t, v.Validate(big), "too many memo fields")
}
