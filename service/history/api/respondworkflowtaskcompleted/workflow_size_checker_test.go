package respondworkflowtaskcompleted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestWorkflowSizeChecker_NumChildWorkflows(t *testing.T) {

	for _, c := range []struct {
		Name                      string
		NumPendingChildExecutions int
		NumPendingActivities      int
		NumPendingCancelRequests  int
		NumPendingSignals         int

		PendingChildExecutionsLimit int
		PendingActivitiesLimit      int
		PendingCancelRequestsLimit  int
		PendingSignalsLimit         int

		ExpectedMetric                  string
		ExpectedChildExecutionsErrorMsg string
		ExpectedActivitiesErrorMsg      string
		ExpectedCancelRequestsErrorMsg  string
		ExpectedSignalsErrorMsg         string
	}{
		{
			Name: "No limits and no data",
		},
		{
			Name:                        "Limits but no workflow data",
			PendingChildExecutionsLimit: 1,
			PendingActivitiesLimit:      1,
			PendingCancelRequestsLimit:  1,
			PendingSignalsLimit:         1,
		},
		{
			Name:                        "Limits not exceeded",
			NumPendingChildExecutions:   1,
			NumPendingActivities:        1,
			NumPendingCancelRequests:    1,
			NumPendingSignals:           1,
			PendingChildExecutionsLimit: 2,
			PendingActivitiesLimit:      2,
			PendingCancelRequestsLimit:  2,
			PendingSignalsLimit:         2,
		},
		{
			Name:                        "Pending child executions limit exceeded",
			NumPendingChildExecutions:   1,
			PendingChildExecutionsLimit: 1,
			ExpectedMetric:              "wf_too_many_pending_child_workflows",
			ExpectedChildExecutionsErrorMsg: "the number of pending child workflow executions, 1, has reached the " +
				"per-workflow limit of 1",
		},
		{
			Name:                       "Pending activities limit exceeded",
			NumPendingActivities:       1,
			PendingActivitiesLimit:     1,
			ExpectedMetric:             "wf_too_many_pending_activities",
			ExpectedActivitiesErrorMsg: "the number of pending activities, 1, has reached the per-workflow limit of 1",
		},
		{
			Name:                       "Pending cancel requests limit exceeded",
			NumPendingCancelRequests:   1,
			PendingCancelRequestsLimit: 1,
			ExpectedMetric:             "wf_too_many_pending_cancel_requests",
			ExpectedCancelRequestsErrorMsg: "the number of pending requests to cancel external workflows, 1, has " +
				"reached the per-workflow limit of 1",
		},
		{
			Name:                "Pending signals limit exceeded",
			NumPendingSignals:   1,
			PendingSignalsLimit: 1,
			ExpectedMetric:      "wf_too_many_pending_external_workflow_signals",
			ExpectedSignalsErrorMsg: "the number of pending signals to external workflows, 1, has reached the " +
				"per-workflow limit of 1",
		},
	} {
		t.Run(c.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mutableState := historyi.NewMockMutableState(ctrl)
			logger := log.NewMockLogger(ctrl)
			metricsHandler := metrics.NewMockHandler(ctrl)

			workflowKey := definition.NewWorkflowKey(
				"test-namespace-id",
				"test-workflow-id",
				"test-run-id",
			)
			mutableState.EXPECT().GetWorkflowKey().Return(workflowKey).AnyTimes()

			executionInfos := make(map[int64]*persistencespb.ChildExecutionInfo)
			activityInfos := make(map[int64]*persistencespb.ActivityInfo)
			requestCancelInfos := make(map[int64]*persistencespb.RequestCancelInfo)
			signalInfos := make(map[int64]*persistencespb.SignalInfo)
			for i := 0; i < c.NumPendingChildExecutions; i++ {
				executionInfos[int64(i)] = new(persistencespb.ChildExecutionInfo)
			}
			for i := 0; i < c.NumPendingActivities; i++ {
				activityInfos[int64(i)] = new(persistencespb.ActivityInfo)
			}
			for i := 0; i < c.NumPendingCancelRequests; i++ {
				requestCancelInfos[int64(i)] = new(persistencespb.RequestCancelInfo)
			}
			for i := 0; i < c.NumPendingSignals; i++ {
				signalInfos[int64(i)] = new(persistencespb.SignalInfo)
			}
			mutableState.EXPECT().GetPendingChildExecutionInfos().Return(executionInfos)
			mutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
			mutableState.EXPECT().GetPendingRequestCancelExternalInfos().Return(requestCancelInfos)
			mutableState.EXPECT().GetPendingSignalExternalInfos().Return(signalInfos)

			if len(c.ExpectedMetric) > 0 {
				counterMetric := metrics.NewMockCounterIface(ctrl)
				metricsHandler.EXPECT().Counter(c.ExpectedMetric).Return(counterMetric)
				counterMetric.EXPECT().Record(int64(1))
			}

			for _, msg := range []string{
				c.ExpectedChildExecutionsErrorMsg,
				c.ExpectedActivitiesErrorMsg,
				c.ExpectedCancelRequestsErrorMsg,
				c.ExpectedSignalsErrorMsg,
			} {
				if len(msg) > 0 {
					logger.EXPECT().Error(msg, gomock.Any()).Do(func(msg string, tags ...tag.Tag) {
						var namespaceID, workflowID, runID interface{}
						for _, t := range tags {
							if t.Key() == "wf-namespace-id" {
								namespaceID = t.Value()
							} else if t.Key() == "wf-id" {
								workflowID = t.Value()
							} else if t.Key() == "wf-run-id" {
								runID = t.Value()
							}
						}
						assert.Equal(t, "test-namespace-id", namespaceID)
						assert.Equal(t, "test-workflow-id", workflowID)
						assert.Equal(t, "test-run-id", runID)
					})
				}
			}

			checker := newWorkflowSizeChecker(workflowSizeLimits{
				numPendingChildExecutionsLimit: c.PendingChildExecutionsLimit,
				numPendingActivitiesLimit:      c.PendingActivitiesLimit,
				numPendingCancelsRequestLimit:  c.PendingCancelRequestsLimit,
				numPendingSignalsLimit:         c.PendingSignalsLimit,
			}, mutableState, nil, metricsHandler, logger)

			err := checker.checkIfNumChildWorkflowsExceedsLimit()
			if len(c.ExpectedChildExecutionsErrorMsg) > 0 {
				require.Error(t, err)
				assert.Equal(t, c.ExpectedChildExecutionsErrorMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}

			err = checker.checkIfNumPendingActivitiesExceedsLimit()
			if len(c.ExpectedActivitiesErrorMsg) > 0 {
				require.Error(t, err)
				assert.Equal(t, c.ExpectedActivitiesErrorMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}

			err = checker.checkIfNumPendingCancelRequestsExceedsLimit()
			if len(c.ExpectedCancelRequestsErrorMsg) > 0 {
				require.Error(t, err)
				assert.Equal(t, c.ExpectedCancelRequestsErrorMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}

			err = checker.checkIfNumPendingSignalsExceedsLimit()
			if len(c.ExpectedSignalsErrorMsg) > 0 {
				require.Error(t, err)
				assert.Equal(t, c.ExpectedSignalsErrorMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
