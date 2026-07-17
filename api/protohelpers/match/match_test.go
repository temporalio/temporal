package match_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/protohelpers/match"
)

// recorder captures a single require.Fail invocation without aborting the test.
type recorder struct {
	failed bool
	msg    string
}

func (r *recorder) Errorf(format string, args ...any) { r.msg += fmt.Sprintf(format, args...) }
func (r *recorder) FailNow()                          { r.failed = true }

// ignoreAll returns a matcher that ignores every field, as a base for tests to
// override only the fields they care about.
func ignoreAll() match.StartWorkflowExecutionRequest {
	return match.StartWorkflowExecutionRequest{
		Namespace:                    match.Any(),
		WorkflowId:                   match.Any(),
		WorkflowType:                 match.Any(),
		TaskQueue:                    match.Any(),
		Input:                        match.Any(),
		WorkflowExecutionTimeout:     match.Any(),
		WorkflowRunTimeout:           match.Any(),
		WorkflowTaskTimeout:          match.Any(),
		Identity:                     match.Any(),
		RequestId:                    match.Any(),
		WorkflowIdReusePolicy:        match.Any(),
		WorkflowIdConflictPolicy:     match.Any(),
		RetryPolicy:                  match.Any(),
		CronSchedule:                 match.Any(),
		Memo:                         match.Any(),
		SearchAttributes:             match.Any(),
		Header:                       match.Any(),
		RequestEagerExecution:        match.Any(),
		ContinuedFailure:             match.Any(),
		LastCompletionResult:         match.Any(),
		WorkflowStartDelay:           match.Any(),
		CompletionCallbacks:          match.Any(),
		UserMetadata:                 match.Any(),
		Links:                        match.Any(),
		VersioningOverride:           match.Any(),
		OnConflictOptions:            match.Any(),
		Priority:                     match.Any(),
		EagerWorkerDeploymentOptions: match.Any(),
		TimeSkippingConfig:           match.Any(),
	}
}

func TestMatch_Pass(t *testing.T) {
	actual := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    "ns",
		WorkflowId:   "wf-1",
		WorkflowType: &commonpb.WorkflowType{Name: "T"},
		Identity:     "worker",
		Links:        []*commonpb.Link{{}},
	}

	m := ignoreAll()
	m.Namespace = match.Eq("ns")                           // literal via matcher
	m.WorkflowId = match.NotEmpty()                        // predicate
	m.WorkflowType = &commonpb.WorkflowType{Name: "T"}     // bare proto literal -> proto-aware Eq
	m.Identity = "worker"                                  // bare scalar literal
	m.RequestId = match.IsEmpty()                          // unset field
	m.Links = match.NotEmpty()                             // repeated
	m.WorkflowIdReusePolicy = match.AnyOf(match.IsEmpty()) // any-of

	r := &recorder{}
	m.Equal(r, actual)
	require.False(t, r.failed, r.msg)
}

func TestMatch_Mismatch(t *testing.T) {
	actual := &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns"}

	m := ignoreAll()
	m.Namespace = match.Eq("different")
	m.WorkflowId = match.NotEmpty() // actual is empty -> also fails

	r := &recorder{}
	m.Equal(r, actual)
	require.True(t, r.failed)
	require.Contains(t, r.msg, "namespace")
	require.Contains(t, r.msg, "workflow_id")
}

func TestMatch_ExhaustiveRequiresEveryField(t *testing.T) {
	actual := &workflowservice.StartWorkflowExecutionRequest{}

	// Only one field specified; all others are nil and must fail.
	m := match.StartWorkflowExecutionRequest{Namespace: match.Any()}

	r := &recorder{}
	m.Equal(r, actual)
	require.True(t, r.failed)
	require.Contains(t, r.msg, "no matcher specified")
	require.Contains(t, r.msg, "workflow_id")
	require.NotContains(t, r.msg, "namespace:", "the specified field must not be flagged")
}

func TestMatch_Partial(t *testing.T) {
	actual := &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns", WorkflowId: "wf-1"}

	// Only the specified fields are checked; the rest are ignored (no Any() needed).
	r := &recorder{}
	match.StartWorkflowExecutionRequest{
		Namespace:  match.Eq("ns"),
		WorkflowId: match.NotEmpty(),
	}.EqualPartial(r, actual)
	require.False(t, r.failed, r.msg)

	// A specified field that mismatches still fails in partial mode.
	r2 := &recorder{}
	match.StartWorkflowExecutionRequest{Namespace: match.Eq("other")}.EqualPartial(r2, actual)
	require.True(t, r2.failed)
	require.Contains(t, r2.msg, "namespace")

	// match.Any() is a misuse in partial mode (ignore by omission instead).
	r3 := &recorder{}
	match.StartWorkflowExecutionRequest{Namespace: match.Any()}.EqualPartial(r3, actual)
	require.True(t, r3.failed)
	require.Contains(t, r3.msg, "redundant in EqualPartial")
}

func TestMatch_Nested(t *testing.T) {
	actual := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    "ns",
		WorkflowType: &commonpb.WorkflowType{Name: "MyWorkflow"},
	}

	// Match the nested WorkflowType's fields with predicates, ignoring the rest
	// of the request via partial mode.
	r := &recorder{}
	match.StartWorkflowExecutionRequest{
		Namespace:    "ns",
		WorkflowType: match.Nested(match.WorkflowType{Name: match.NotEmpty()}),
	}.EqualPartial(r, actual)
	require.False(t, r.failed, r.msg)

	// A nested field mismatch is reported.
	r2 := &recorder{}
	match.StartWorkflowExecutionRequest{
		WorkflowType: match.Nested(match.WorkflowType{Name: match.Eq("Other")}),
	}.EqualPartial(r2, actual)
	require.True(t, r2.failed)
	require.Contains(t, r2.msg, "workflow_type")
	require.Contains(t, r2.msg, "nested mismatch")

	// A nil nested message fails.
	r3 := &recorder{}
	match.StartWorkflowExecutionRequest{
		WorkflowType: match.Nested(match.WorkflowType{Name: match.Any()}),
	}.EqualPartial(r3, &workflowservice.StartWorkflowExecutionRequest{})
	require.True(t, r3.failed)
	require.Contains(t, r3.msg, "expected a non-nil message")
}

func TestMatch_Map(t *testing.T) {
	memo := &commonpb.Memo{Fields: map[string]*commonpb.Payload{"k": {}}}

	r := &recorder{}
	match.Memo{Fields: match.NotEmpty()}.Equal(r, memo)
	require.False(t, r.failed, r.msg)

	r2 := &recorder{}
	match.Memo{Fields: match.IsEmpty()}.Equal(r2, memo)
	require.True(t, r2.failed)
}

func TestMatch_Oneof(t *testing.T) {
	cmd := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{
			StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{TimerId: "t"},
		},
	}

	r := &recorder{}
	match.Command{
		CommandType:       match.Eq(enumspb.COMMAND_TYPE_START_TIMER),
		UserMetadata:      match.IsEmpty(),
		EventGroupMarkers: match.IsEmpty(),
		Attributes:        match.NotEmpty(), // oneof member is set
	}.Equal(r, cmd)
	require.False(t, r.failed, r.msg)

	r2 := &recorder{}
	match.Command{
		CommandType:       match.Any(),
		UserMetadata:      match.Any(),
		EventGroupMarkers: match.Any(),
		Attributes:        match.IsEmpty(), // but it is set -> fails
	}.Equal(r2, cmd)
	require.True(t, r2.failed)
	require.Contains(t, r2.msg, "attributes")
}
