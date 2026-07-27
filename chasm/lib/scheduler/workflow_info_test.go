package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

type workflowInfoFieldPolicy int

const (
	workflowInfoFieldForwarded workflowInfoFieldPolicy = iota
	workflowInfoFieldTransformed
	workflowInfoFieldUnsupported
)

var workflowInfoFieldPolicies = map[protoreflect.Name]workflowInfoFieldPolicy{
	"workflow_id":                workflowInfoFieldTransformed,
	"workflow_type":              workflowInfoFieldForwarded,
	"task_queue":                 workflowInfoFieldForwarded,
	"input":                      workflowInfoFieldForwarded,
	"workflow_execution_timeout": workflowInfoFieldForwarded,
	"workflow_run_timeout":       workflowInfoFieldForwarded,
	"workflow_task_timeout":      workflowInfoFieldForwarded,
	"workflow_id_reuse_policy":   workflowInfoFieldTransformed,
	"retry_policy":               workflowInfoFieldForwarded,
	"cron_schedule":              workflowInfoFieldUnsupported,
	"memo":                       workflowInfoFieldForwarded,
	"search_attributes":          workflowInfoFieldTransformed,
	"header":                     workflowInfoFieldForwarded,
	"user_metadata":              workflowInfoFieldForwarded,
	"versioning_override":        workflowInfoFieldForwarded,
	"priority":                   workflowInfoFieldForwarded,
}

func TestNewWorkflowExecutionInfoFieldPoliciesAreComplete(t *testing.T) {
	fields := (&workflowpb.NewWorkflowExecutionInfo{}).ProtoReflect().Descriptor().Fields()
	require.Len(t, workflowInfoFieldPolicies, fields.Len())
	for i := range fields.Len() {
		field := fields.Get(i)
		require.Contains(t, workflowInfoFieldPolicies, field.Name())
	}
	for name := range workflowInfoFieldPolicies {
		require.NotNil(t, fields.ByName(name))
	}
}

func TestApplyNewWorkflowExecutionInfo(t *testing.T) {
	info := &workflowpb.NewWorkflowExecutionInfo{
		WorkflowId:               "action-workflow-id",
		WorkflowType:             &commonpb.WorkflowType{Name: "workflow-type"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: "task-queue"},
		Input:                    &commonpb.Payloads{},
		WorkflowExecutionTimeout: durationpb.New(time.Hour),
		WorkflowRunTimeout:       durationpb.New(time.Minute),
		WorkflowTaskTimeout:      durationpb.New(time.Second),
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		RetryPolicy:              &commonpb.RetryPolicy{MaximumAttempts: 3},
		CronSchedule:             "@daily",
		Memo:                     &commonpb.Memo{},
		SearchAttributes:         &commonpb.SearchAttributes{},
		Header:                   &commonpb.Header{},
		UserMetadata:             &sdkpb.UserMetadata{},
		VersioningOverride:       &workflowpb.VersioningOverride{},
		Priority:                 &commonpb.Priority{},
	}
	searchAttributes := &commonpb.SearchAttributes{}
	request := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:            "scheduler-workflow-id",
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		SearchAttributes:      searchAttributes,
	}

	applyNewWorkflowExecutionInfo(request, info)

	infoMessage := info.ProtoReflect()
	requestMessage := request.ProtoReflect()
	for name, policy := range workflowInfoFieldPolicies {
		if policy != workflowInfoFieldForwarded {
			continue
		}
		infoField := infoMessage.Descriptor().Fields().ByName(name)
		requestField := requestMessage.Descriptor().Fields().ByName(name)
		require.NotNil(t, requestField)
		require.True(t, infoMessage.Has(infoField), "populate forwarded field %q in the test fixture", name)
		require.True(t, requestMessage.Has(requestField), "forward field %q", name)
		require.False(t, infoField.IsList() || infoField.IsMap(), "extend the assertion for collection field %q", name)
		if infoField.Kind() == protoreflect.MessageKind {
			require.True(t, proto.Equal(
				infoMessage.Get(infoField).Message().Interface(),
				requestMessage.Get(requestField).Message().Interface(),
			), "forward field %q unchanged", name)
		} else {
			require.Equal(t, infoMessage.Get(infoField).Interface(), requestMessage.Get(requestField).Interface())
		}
	}

	protorequire.ProtoEqual(t, &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               "scheduler-workflow-id",
		WorkflowType:             info.WorkflowType,
		TaskQueue:                info.TaskQueue,
		Input:                    info.Input,
		WorkflowExecutionTimeout: info.WorkflowExecutionTimeout,
		WorkflowRunTimeout:       info.WorkflowRunTimeout,
		WorkflowTaskTimeout:      info.WorkflowTaskTimeout,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		RetryPolicy:              info.RetryPolicy,
		Memo:                     info.Memo,
		SearchAttributes:         searchAttributes,
		Header:                   info.Header,
		UserMetadata:             info.UserMetadata,
		VersioningOverride:       info.VersioningOverride,
		Priority:                 info.Priority,
	}, request)
}
