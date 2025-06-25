package workers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/sqlquery"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestActivityInfoMatchEvaluator_LogicalOperations(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedMatch bool
		expectedError bool
	}{
		{
			name:          "test AND, true",
			query:         fmt.Sprintf("%s = 'task_queue' AND %s = 'Running'", workerTaskQueueColName, workerStatusColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test AND, false, left cause",
			query:         fmt.Sprintf("%s = 'task_queue_unknown' AND %s = 'Running'", workerTaskQueueColName, workerStatusColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "test and, false, right cause",
			query:         fmt.Sprintf("%s = 'task_queue' AND %s = 'NotRunning'", workerTaskQueueColName, workerStatusColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "test OR, true",
			query:         fmt.Sprintf("%s = 'task_queue' OR %s = 'Running'", workerTaskQueueColName, workerStatusColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test OR, true, left only",
			query:         fmt.Sprintf("%s = 'task_queue' OR %s = 'NotRunning'", workerTaskQueueColName, workerStatusColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test OR, true, right only",
			query:         fmt.Sprintf("%s = 'task_queue_unknown' OR %s = 'Running'", workerTaskQueueColName, workerStatusColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test OR, false",
			query:         fmt.Sprintf("%s = 'task_queue_unknown' OR %s = 'NotRunning'", workerTaskQueueColName, workerStatusColName),
			expectedMatch: false,
			expectedError: false,
		},
	}

	hb := &workerpb.WorkerHeartbeat{
		TaskQueue: "task_queue",
		Status:    enumspb.WORKER_STATUS_RUNNING,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := newWorkerQueryEngine("nsID", tt.query)
			assert.NoError(t, err)

			match, err := engine.EvaluateWorker(hb)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}
}

func TestActivityInfoMatchEvaluator_SupportedFields(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedMatch bool
	}{
		{
			name:          "TaskQueue, true",
			query:         fmt.Sprintf("%s = 'task_queue'", workerTaskQueueColName),
			expectedMatch: true,
		},
		{
			name:          "TaskQueue, false",
			query:         fmt.Sprintf("%s = 'task_queue_unknown'", workerTaskQueueColName),
			expectedMatch: false,
		},
		{
			name:          "WorkerInstanceKey, true",
			query:         fmt.Sprintf("%s = 'worker_key'", workerInstanceKeyColName),
			expectedMatch: true,
		},
		{
			name:          "WorkerInstanceKey, false",
			query:         fmt.Sprintf("%s = 'worker_key_unknown'", workerInstanceKeyColName),
			expectedMatch: false,
		},
		{
			name:          "WorkerIdentity, true",
			query:         fmt.Sprintf("%s = 'worker_identity'", workerIdentityColName),
			expectedMatch: true,
		},
		{
			name:          "WorkerIdentity, false",
			query:         fmt.Sprintf("%s = 'worker_identity_unknown'", workerIdentityColName),
			expectedMatch: false,
		},
		{
			name:          "HostName, true",
			query:         fmt.Sprintf("%s = 'host_name'", workerHostNameColName),
			expectedMatch: true,
		},
		{
			name:          "HostName, false",
			query:         fmt.Sprintf("%s = 'host_name_unknown'", workerHostNameColName),
			expectedMatch: false,
		},
		{
			name:          "DeploymentName, true",
			query:         fmt.Sprintf("%s = 'deployment_name'", workerDeploymentNameColName),
			expectedMatch: true,
		},
		{
			name:          "DeploymentName, false",
			query:         fmt.Sprintf("%s = 'deployment_name_unknown'", workerDeploymentNameColName),
			expectedMatch: false,
		},
		{
			name:          "SdkName, true",
			query:         fmt.Sprintf("%s = 'sdk_name'", workerSdkNameColName),
			expectedMatch: true,
		},
		{
			name:          "SdkName, false",
			query:         fmt.Sprintf("%s = 'sdk_name_unknown'", workerSdkNameColName),
			expectedMatch: false,
		},
		{
			name:          "SdkVersion, true",
			query:         fmt.Sprintf("%s = 'sdk_version'", workerSdkVersionColName),
			expectedMatch: true,
		},
		{
			name:          "SdkVersion, false",
			query:         fmt.Sprintf("%s = 'sdk_version_unknown'", workerSdkVersionColName),
			expectedMatch: false,
		},
		{
			name:          "WorkerStatus, true",
			query:         fmt.Sprintf("%s = 'Running'", workerStatusColName),
			expectedMatch: true,
		},
		{
			name:          "WorkerStatus, false",
			query:         fmt.Sprintf("%s = 'status_unknown'", workerStatusColName),
			expectedMatch: false,
		},
	}

	hb := &workerpb.WorkerHeartbeat{
		TaskQueue:         "task_queue",
		Status:            enumspb.WORKER_STATUS_RUNNING,
		WorkerIdentity:    "worker_identity",
		WorkerInstanceKey: "worker_key",
		HostInfo: &workerpb.WorkerHostInfo{
			HostName: "host_name",
		},
		DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
			DeploymentName: "deployment_name",
			BuildId:        "build_id",
		},
		SdkName:    "sdk_name",
		SdkVersion: "sdk_version",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := newWorkerQueryEngine("nsID", tt.query)
			assert.NoError(t, err)

			match, err := engine.EvaluateWorker(hb)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedMatch, match)
		})
	}
}

func TestActivityInfoMatchEvaluator_SupportedTimeFields(t *testing.T) {
	startTimeStr := "2023-10-26T14:30:00Z"
	beforeTimeStr := "2023-10-26T13:00:00Z"
	afterTimeStr := "2023-10-26T15:00:00Z"

	tests := []struct {
		name          string
		query         string
		expectedMatch bool
	}{
		{
			name:          "worker time, equal clause",
			query:         "%s" + fmt.Sprintf(" = '%s'", startTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker time, >= clause, equal",
			query:         "%s" + fmt.Sprintf(" >= '%s'", startTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker time, >= clause, after",
			query:         "%s" + fmt.Sprintf(" >= '%s'", afterTimeStr),
			expectedMatch: false,
		},
		{
			name:          "worker start time, >= clause, before",
			query:         "%s" + fmt.Sprintf(" >= '%s'", beforeTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker start time, > clause, equal",
			query:         "%s" + fmt.Sprintf(" > '%s'", startTimeStr),
			expectedMatch: false,
		},
		{
			name:          "worker start time, > clause, after",
			query:         "%s" + fmt.Sprintf(" > '%s'", afterTimeStr),
			expectedMatch: false,
		},
		{
			name:          "worker start time, > clause, before",
			query:         "%s" + fmt.Sprintf(" >= '%s'", beforeTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker start time, <= clause, equal",
			query:         "%s" + fmt.Sprintf(" <= '%s'", startTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker start time, <= clause, after",
			query:         "%s" + fmt.Sprintf(" <= '%s'", afterTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker start time, <= clause, before",
			query:         "%s" + fmt.Sprintf(" <= '%s'", beforeTimeStr),
			expectedMatch: false,
		},
		{
			name:          "worker start time < clause, equal",
			query:         "%s" + fmt.Sprintf(" < '%s'", startTimeStr),
			expectedMatch: false,
		},
		{
			name:          "worker start time, < clause, after",
			query:         "%s" + fmt.Sprintf(" < '%s'", afterTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker start time where < clause, before",
			query:         "%s" + fmt.Sprintf(" < '%s'", beforeTimeStr),
			expectedMatch: false,
		},
		{
			name:          "worker start time between clause, match",
			query:         "%s" + fmt.Sprintf(" between '%s' and '%s'", beforeTimeStr, afterTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker start time between clause, miss",
			query:         "%s" + fmt.Sprintf(" between '%s' and '%s'", afterTimeStr, "2023-10-26T16:00:00Z"),
			expectedMatch: false,
		},
		{
			name:          "worker start time between clause, inclusive left",
			query:         "%s" + fmt.Sprintf(" between '%s' and '%s'", beforeTimeStr, startTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker start time between clause, inclusive right",
			query:         "%s" + fmt.Sprintf(" between '%s' and '%s'", startTimeStr, afterTimeStr),
			expectedMatch: true,
		},
		{
			name:          "worker start time not between clause, miss",
			query:         "%s" + fmt.Sprintf(" not between '%s' and '%s'", beforeTimeStr, afterTimeStr),
			expectedMatch: false,
		},
		{
			name:          "worker start time not between clause, match",
			query:         "%s" + fmt.Sprintf(" not between '%s' and '%s'", afterTimeStr, "2023-10-26T16:00:00Z"),
			expectedMatch: true,
		},
	}
	startTime, err := sqlquery.ConvertToTime(fmt.Sprintf("'%s'", startTimeStr))
	assert.NoError(t, err)

	hb := &workerpb.WorkerHeartbeat{
		StartTime:     timestamppb.New(startTime),
		HeartbeatTime: timestamppb.New(startTime),
		TaskQueue:     "task_queue",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, colName := range []string{workerStartTimeColName, workerHeartbeatTimeColName} {
				query := fmt.Sprintf(tt.query, colName)
				engine, err := newWorkerQueryEngine("nsID", query)
				assert.NoError(t, err)
				match, err := engine.EvaluateWorker(hb)
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}
}
