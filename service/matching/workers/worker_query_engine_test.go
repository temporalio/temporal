package workers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			name:          "BuildId, true",
			query:         fmt.Sprintf("%s = 'build_id'", workerBuildIDColName),
			expectedMatch: true,
		},
		{
			name:          "BuildId, false",
			query:         fmt.Sprintf("%s = 'build_id_unknown'", workerBuildIDColName),
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
		{
			name:          "Status (alias), true",
			query:         "Status = 'Running'",
			expectedMatch: true,
		},
		{
			name:          "Status (alias), false",
			query:         "Status = 'status_unknown'",
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

func TestWorkerQueryEngine_IsNullString(t *testing.T) {
	hbWithDeployment := &workerpb.WorkerHeartbeat{
		TaskQueue: "task_queue",
		DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
			DeploymentName: "my-deployment",
		},
	}
	hbWithoutDeployment := &workerpb.WorkerHeartbeat{
		TaskQueue: "task_queue",
	}

	engine, err := newWorkerQueryEngine("nsID", fmt.Sprintf("%s IS NULL", workerDeploymentNameColName))
	require.NoError(t, err)

	match, err := engine.EvaluateWorker(hbWithoutDeployment)
	require.NoError(t, err)
	assert.True(t, match, "IS NULL should match when DeploymentName is empty")

	match, err = engine.EvaluateWorker(hbWithDeployment)
	require.NoError(t, err)
	assert.False(t, match, "IS NULL should not match when DeploymentName is set")
}

func TestWorkerQueryEngine_IsNotNullString(t *testing.T) {
	hbWithDeployment := &workerpb.WorkerHeartbeat{
		TaskQueue: "task_queue",
		DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
			DeploymentName: "my-deployment",
		},
	}
	hbWithoutDeployment := &workerpb.WorkerHeartbeat{
		TaskQueue: "task_queue",
	}

	engine, err := newWorkerQueryEngine("nsID", fmt.Sprintf("%s IS NOT NULL", workerDeploymentNameColName))
	require.NoError(t, err)

	match, err := engine.EvaluateWorker(hbWithDeployment)
	require.NoError(t, err)
	assert.True(t, match, "IS NOT NULL should match when DeploymentName is set")

	match, err = engine.EvaluateWorker(hbWithoutDeployment)
	require.NoError(t, err)
	assert.False(t, match, "IS NOT NULL should not match when DeploymentName is empty")
}

func TestWorkerQueryEngine_IsNullTime(t *testing.T) {
	startTimeStr := "2023-10-26T14:30:00Z"
	startTime, err := sqlquery.ConvertToTime(fmt.Sprintf("'%s'", startTimeStr))
	require.NoError(t, err)

	hbWithTime := &workerpb.WorkerHeartbeat{
		TaskQueue:     "task_queue",
		StartTime:     timestamppb.New(startTime),
		HeartbeatTime: timestamppb.New(startTime),
	}
	hbWithoutTime := &workerpb.WorkerHeartbeat{
		TaskQueue: "task_queue",
	}

	for _, colName := range []string{workerStartTimeColName, workerHeartbeatTimeColName} {
		engine, err := newWorkerQueryEngine("nsID", fmt.Sprintf("%s IS NULL", colName))
		require.NoError(t, err)

		t.Run(fmt.Sprintf("%s matches when not set", colName), func(t *testing.T) {
			match, err := engine.EvaluateWorker(hbWithoutTime)
			require.NoError(t, err)
			assert.True(t, match)
		})

		t.Run(fmt.Sprintf("%s does not match when set", colName), func(t *testing.T) {
			match, err := engine.EvaluateWorker(hbWithTime)
			require.NoError(t, err)
			assert.False(t, match)
		})
	}
}

func TestWorkerQueryEngine_IsNotNullTime(t *testing.T) {
	startTimeStr := "2023-10-26T14:30:00Z"
	startTime, err := sqlquery.ConvertToTime(fmt.Sprintf("'%s'", startTimeStr))
	require.NoError(t, err)

	hbWithTime := &workerpb.WorkerHeartbeat{
		TaskQueue:     "task_queue",
		StartTime:     timestamppb.New(startTime),
		HeartbeatTime: timestamppb.New(startTime),
	}
	hbWithoutTime := &workerpb.WorkerHeartbeat{
		TaskQueue: "task_queue",
	}

	for _, colName := range []string{workerStartTimeColName, workerHeartbeatTimeColName} {
		engine, err := newWorkerQueryEngine("nsID", fmt.Sprintf("%s IS NOT NULL", colName))
		require.NoError(t, err)

		t.Run(fmt.Sprintf("%s matches when set", colName), func(t *testing.T) {
			match, err := engine.EvaluateWorker(hbWithTime)
			require.NoError(t, err)
			assert.True(t, match)
		})

		t.Run(fmt.Sprintf("%s does not match when not set", colName), func(t *testing.T) {
			match, err := engine.EvaluateWorker(hbWithoutTime)
			require.NoError(t, err)
			assert.False(t, match)
		})
	}
}

func TestWorkerQueryEngine_IsNullComposite(t *testing.T) {
	hb := &workerpb.WorkerHeartbeat{
		TaskQueue: "task_queue",
		SdkName:   "temporal-go",
	}

	tests := []struct {
		name          string
		query         string
		expectedMatch bool
	}{
		{
			name:          "IS NOT NULL AND equality, both match",
			query:         fmt.Sprintf("%s IS NOT NULL AND %s = 'task_queue'", workerSdkNameColName, workerTaskQueueColName),
			expectedMatch: true,
		},
		{
			name:          "IS NULL OR equality, right matches",
			query:         fmt.Sprintf("%s IS NULL OR %s = 'task_queue'", workerSdkNameColName, workerTaskQueueColName),
			expectedMatch: true,
		},
		{
			name:          "IS NULL AND equality, left fails",
			query:         fmt.Sprintf("%s IS NULL AND %s = 'task_queue'", workerSdkNameColName, workerTaskQueueColName),
			expectedMatch: false,
		},
		{
			name:          "IS NULL with empty field in composite",
			query:         fmt.Sprintf("%s IS NULL AND %s IS NOT NULL", workerDeploymentNameColName, workerSdkNameColName),
			expectedMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := newWorkerQueryEngine("nsID", tt.query)
			require.NoError(t, err)
			match, err := engine.EvaluateWorker(hb)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedMatch, match)
		})
	}
}

func TestWorkerQueryEngine_IsNullRejectsUnknownColumn(t *testing.T) {
	hb := &workerpb.WorkerHeartbeat{TaskQueue: "task_queue"}
	engine, err := newWorkerQueryEngine("nsID", "UnknownField IS NULL")
	require.NoError(t, err)
	_, err = engine.EvaluateWorker(hb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown or unsupported")
}

// Only IS NULL and IS NOT NULL are supported; other IS operators (e.g., IS TRUE) should be rejected.
func TestWorkerQueryEngine_IsExprRejectsUnsupportedOperators(t *testing.T) {
	hb := &workerpb.WorkerHeartbeat{TaskQueue: "task_queue"}
	engine, err := newWorkerQueryEngine("nsID", fmt.Sprintf("%s IS TRUE", workerTaskQueueColName))
	require.NoError(t, err)
	_, err = engine.EvaluateWorker(hb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}
