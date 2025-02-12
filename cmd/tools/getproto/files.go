
// Code generated by getproto. DO NOT EDIT.
// If you get build errors in this file, just delete it. It will be regenerated.

package main

import (
	"google.golang.org/protobuf/reflect/protoreflect"

	activity "go.temporal.io/api/activity/v1"
	batch "go.temporal.io/api/batch/v1"
	command "go.temporal.io/api/command/v1"
	common "go.temporal.io/api/common/v1"
	deployment "go.temporal.io/api/deployment/v1"
	enums "go.temporal.io/api/enums/v1"
	failure "go.temporal.io/api/failure/v1"
	filter "go.temporal.io/api/filter/v1"
	history "go.temporal.io/api/history/v1"
	namespace "go.temporal.io/api/namespace/v1"
	nexus "go.temporal.io/api/nexus/v1"
	protocol "go.temporal.io/api/protocol/v1"
	query "go.temporal.io/api/query/v1"
	replication "go.temporal.io/api/replication/v1"
	rules "go.temporal.io/api/rules/v1"
	schedule "go.temporal.io/api/schedule/v1"
	sdk "go.temporal.io/api/sdk/v1"
	taskqueue "go.temporal.io/api/taskqueue/v1"
	update "go.temporal.io/api/update/v1"
	version "go.temporal.io/api/version/v1"
	workflow "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	anypb "google.golang.org/protobuf/types/known/anypb"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	fieldmaskpb "google.golang.org/protobuf/types/known/fieldmaskpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

func init() {
	importMap = make(map[string]protoreflect.FileDescriptor)
	importMap["google/protobuf/any.proto"] = anypb.File_google_protobuf_any_proto
	importMap["google/protobuf/duration.proto"] = durationpb.File_google_protobuf_duration_proto
	importMap["google/protobuf/empty.proto"] = emptypb.File_google_protobuf_empty_proto
	importMap["google/protobuf/field_mask.proto"] = fieldmaskpb.File_google_protobuf_field_mask_proto
	importMap["google/protobuf/timestamp.proto"] = timestamppb.File_google_protobuf_timestamp_proto
	importMap["google/protobuf/wrappers.proto"] = wrapperspb.File_google_protobuf_wrappers_proto
	importMap["temporal/api/activity/v1/message.proto"] = activity.File_temporal_api_activity_v1_message_proto
	importMap["temporal/api/batch/v1/message.proto"] = batch.File_temporal_api_batch_v1_message_proto
	importMap["temporal/api/command/v1/message.proto"] = command.File_temporal_api_command_v1_message_proto
	importMap["temporal/api/common/v1/message.proto"] = common.File_temporal_api_common_v1_message_proto
	importMap["temporal/api/deployment/v1/message.proto"] = deployment.File_temporal_api_deployment_v1_message_proto
	importMap["temporal/api/enums/v1/batch_operation.proto"] = enums.File_temporal_api_enums_v1_batch_operation_proto
	importMap["temporal/api/enums/v1/command_type.proto"] = enums.File_temporal_api_enums_v1_command_type_proto
	importMap["temporal/api/enums/v1/common.proto"] = enums.File_temporal_api_enums_v1_common_proto
	importMap["temporal/api/enums/v1/deployment.proto"] = enums.File_temporal_api_enums_v1_deployment_proto
	importMap["temporal/api/enums/v1/event_type.proto"] = enums.File_temporal_api_enums_v1_event_type_proto
	importMap["temporal/api/enums/v1/failed_cause.proto"] = enums.File_temporal_api_enums_v1_failed_cause_proto
	importMap["temporal/api/enums/v1/namespace.proto"] = enums.File_temporal_api_enums_v1_namespace_proto
	importMap["temporal/api/enums/v1/nexus.proto"] = enums.File_temporal_api_enums_v1_nexus_proto
	importMap["temporal/api/enums/v1/query.proto"] = enums.File_temporal_api_enums_v1_query_proto
	importMap["temporal/api/enums/v1/reset.proto"] = enums.File_temporal_api_enums_v1_reset_proto
	importMap["temporal/api/enums/v1/schedule.proto"] = enums.File_temporal_api_enums_v1_schedule_proto
	importMap["temporal/api/enums/v1/task_queue.proto"] = enums.File_temporal_api_enums_v1_task_queue_proto
	importMap["temporal/api/enums/v1/update.proto"] = enums.File_temporal_api_enums_v1_update_proto
	importMap["temporal/api/enums/v1/workflow.proto"] = enums.File_temporal_api_enums_v1_workflow_proto
	importMap["temporal/api/failure/v1/message.proto"] = failure.File_temporal_api_failure_v1_message_proto
	importMap["temporal/api/filter/v1/message.proto"] = filter.File_temporal_api_filter_v1_message_proto
	importMap["temporal/api/history/v1/message.proto"] = history.File_temporal_api_history_v1_message_proto
	importMap["temporal/api/namespace/v1/message.proto"] = namespace.File_temporal_api_namespace_v1_message_proto
	importMap["temporal/api/nexus/v1/message.proto"] = nexus.File_temporal_api_nexus_v1_message_proto
	importMap["temporal/api/protocol/v1/message.proto"] = protocol.File_temporal_api_protocol_v1_message_proto
	importMap["temporal/api/query/v1/message.proto"] = query.File_temporal_api_query_v1_message_proto
	importMap["temporal/api/replication/v1/message.proto"] = replication.File_temporal_api_replication_v1_message_proto
	importMap["temporal/api/rules/v1/message.proto"] = rules.File_temporal_api_rules_v1_message_proto
	importMap["temporal/api/schedule/v1/message.proto"] = schedule.File_temporal_api_schedule_v1_message_proto
	importMap["temporal/api/sdk/v1/task_complete_metadata.proto"] = sdk.File_temporal_api_sdk_v1_task_complete_metadata_proto
	importMap["temporal/api/sdk/v1/user_metadata.proto"] = sdk.File_temporal_api_sdk_v1_user_metadata_proto
	importMap["temporal/api/taskqueue/v1/message.proto"] = taskqueue.File_temporal_api_taskqueue_v1_message_proto
	importMap["temporal/api/update/v1/message.proto"] = update.File_temporal_api_update_v1_message_proto
	importMap["temporal/api/version/v1/message.proto"] = version.File_temporal_api_version_v1_message_proto
	importMap["temporal/api/workflow/v1/message.proto"] = workflow.File_temporal_api_workflow_v1_message_proto
	importMap["temporal/api/workflowservice/v1/request_response.proto"] = workflowservice.File_temporal_api_workflowservice_v1_request_response_proto
}
