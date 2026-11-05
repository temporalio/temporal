package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// fakePipelineDB begins its pipelined and sequential transactions from fakes,
	// and counts the history nodes appended outside a transaction.
	fakePipelineDB struct {
		sqlplugin.DB
		pipelined         *fakeUpdateTx
		sequential        *fakeUpdateTx
		beginPipelinedErr error
		historyNodes      int
	}

	// fakeUpdateTx answers the calls of a minimal update the way a successful
	// update sees them.
	fakeUpdateTx struct {
		sqlplugin.PipelinedTx
		request *p.InternalUpdateWorkflowExecutionRequest

		updateExecutionsErr error
		commitErr           error

		begun          bool
		historyNodes   int
		lockedCurrent  *sqlplugin.CurrentExecutionsFilter
		updatedCurrent *sqlplugin.CurrentExecutionsRow
		committed      bool
	}
)

var pipelineTestStartTime = time.Unix(1700000000, 0).UTC()

func TestUpdateWorkflowExecutionPipelined_Committed(t *testing.T) {
	testCases := []struct {
		name           string
		setup          func(request *p.InternalUpdateWorkflowExecutionRequest)
		historyNodes   int
		updatesCurrent bool
	}{
		{
			name:           "update current",
			setup:          func(*p.InternalUpdateWorkflowExecutionRequest) {},
			historyNodes:   1,
			updatesCurrent: true,
		},
		{
			name: "ignore current",
			setup: func(request *p.InternalUpdateWorkflowExecutionRequest) {
				request.Mode = p.UpdateWorkflowModeIgnoreCurrent
			},
			historyNodes: 1,
		},
		{
			name: "new workflow events",
			setup: func(request *p.InternalUpdateWorkflowExecutionRequest) {
				events := *request.UpdateWorkflowNewEvents[0]
				request.NewWorkflowNewEvents = []*p.InternalAppendHistoryNodesRequest{&events}
			},
			historyNodes:   2,
			updatesCurrent: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			request := newPipelineTestRequest()
			db := newFakePipelineDB(request)
			tc.setup(request)

			require.NoError(t, updateWithFakes(db, request))
			require.Equal(t, tc.historyNodes, db.pipelined.historyNodes)
			var lockedCurrent *sqlplugin.CurrentExecutionsFilter
			var updatedCurrent *sqlplugin.CurrentExecutionsRow
			if tc.updatesCurrent {
				namespaceID := primitives.MustParseUUID(request.UpdateWorkflowMutation.NamespaceID)
				runID := primitives.MustParseUUID(request.UpdateWorkflowMutation.ExecutionState.RunId)
				lockedCurrent = &sqlplugin.CurrentExecutionsFilter{
					ShardID:     1,
					NamespaceID: namespaceID,
					WorkflowID:  "workflow-id",
					RunID:       runID,
					ArchetypeID: chasm.WorkflowArchetypeID,
				}
				updatedCurrent = &sqlplugin.CurrentExecutionsRow{
					ShardID:          1,
					NamespaceID:      namespaceID,
					WorkflowID:       "workflow-id",
					RunID:            runID,
					ArchetypeID:      chasm.WorkflowArchetypeID,
					CreateRequestID:  "create-request-id",
					StartTime:        &pipelineTestStartTime,
					LastWriteVersion: 7,
					State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					Data:             []byte("execution state"),
					DataEncoding:     enumspb.ENCODING_TYPE_PROTO3.String(),
				}
			}
			require.Equal(t, lockedCurrent, db.pipelined.lockedCurrent)
			require.Equal(t, updatedCurrent, db.pipelined.updatedCurrent)
			require.True(t, db.pipelined.committed)
			require.Zero(t, db.historyNodes)
			require.False(t, db.sequential.begun)
		})
	}
}

func TestUpdateWorkflowExecutionPipelined_RunsSequential(t *testing.T) {
	testCases := []struct {
		name           string
		setup          func(request *p.InternalUpdateWorkflowExecutionRequest, db *fakePipelineDB)
		pipelinedBegun bool
	}{
		{
			name: "rejected",
			setup: func(_ *p.InternalUpdateWorkflowExecutionRequest, db *fakePipelineDB) {
				db.pipelined.commitErr = fmt.Errorf("%w: %w", sqlplugin.ErrPipelineNotWritten, errors.New("division by zero"))
			},
			pipelinedBegun: true,
		},
		{
			name: "record error",
			setup: func(_ *p.InternalUpdateWorkflowExecutionRequest, db *fakePipelineDB) {
				db.pipelined.updateExecutionsErr = errors.New("record failed")
			},
			pipelinedBegun: true,
		},
		{
			name: "new branch",
			setup: func(request *p.InternalUpdateWorkflowExecutionRequest, _ *fakePipelineDB) {
				request.UpdateWorkflowNewEvents[0].IsNewBranch = true
			},
		},
		{
			name: "new workflow events new branch",
			setup: func(request *p.InternalUpdateWorkflowExecutionRequest, _ *fakePipelineDB) {
				events := *request.UpdateWorkflowNewEvents[0]
				events.IsNewBranch = true
				request.NewWorkflowNewEvents = []*p.InternalAppendHistoryNodesRequest{&events}
			},
		},
		{
			name: "bypass current",
			setup: func(request *p.InternalUpdateWorkflowExecutionRequest, _ *fakePipelineDB) {
				request.Mode = p.UpdateWorkflowModeBypassCurrent
			},
		},
		{
			name: "new workflow",
			setup: func(request *p.InternalUpdateWorkflowExecutionRequest, _ *fakePipelineDB) {
				request.NewWorkflowSnapshot = &p.InternalWorkflowSnapshot{
					NamespaceID:        request.UpdateWorkflowMutation.NamespaceID,
					WorkflowID:         request.UpdateWorkflowMutation.WorkflowID,
					ExecutionState:     &persistencespb.WorkflowExecutionState{RunId: primitives.NewUUID().String()},
					ExecutionStateBlob: &commonpb.DataBlob{},
					ExecutionInfoBlob:  &commonpb.DataBlob{},
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			request := newPipelineTestRequest()
			db := newFakePipelineDB(request)
			tc.setup(request, db)

			require.NoError(t, updateWithFakes(db, request))
			require.Equal(t, tc.pipelinedBegun, db.pipelined.begun)
			require.False(t, db.pipelined.committed)
			require.Equal(t, len(request.UpdateWorkflowNewEvents)+len(request.NewWorkflowNewEvents), db.historyNodes+db.sequential.historyNodes)
			require.True(t, db.sequential.committed)
		})
	}
}

func TestUpdateWorkflowExecutionPipelined_UnknownOutcomeIsUnavailable(t *testing.T) {
	request := newPipelineTestRequest()
	db := newFakePipelineDB(request)
	db.pipelined.commitErr = errors.New("connection reset")

	err := updateWithFakes(db, request)
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, "UpdateWorkflowExecution: failed to commit transaction: connection reset", unavailable.Message)
	require.Zero(t, db.historyNodes)
	require.False(t, db.sequential.begun)
}

func TestUpdateWorkflowExecutionPipelined_BeginErrorIsUnavailable(t *testing.T) {
	request := newPipelineTestRequest()
	db := newFakePipelineDB(request)
	db.beginPipelinedErr = errors.New("connection refused")

	err := updateWithFakes(db, request)
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, "UpdateWorkflowExecution: failed to start transaction: connection refused", unavailable.Message)
	require.Zero(t, db.historyNodes)
	require.False(t, db.sequential.begun)
}

func newPipelineTestRequest() *p.InternalUpdateWorkflowExecutionRequest {
	return &p.InternalUpdateWorkflowExecutionRequest{
		ShardID:     1,
		RangeID:     5,
		Mode:        p.UpdateWorkflowModeUpdateCurrent,
		ArchetypeID: chasm.WorkflowArchetypeID,
		UpdateWorkflowMutation: p.InternalWorkflowMutation{
			NamespaceID: primitives.NewUUID().String(),
			WorkflowID:  "workflow-id",
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           primitives.NewUUID().String(),
				CreateRequestId: "create-request-id",
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				StartTime:       timestamppb.New(pipelineTestStartTime),
			},
			ExecutionStateBlob: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte("execution state"),
			},
			ExecutionInfoBlob: &commonpb.DataBlob{},
			NextEventID:       11,
			DBRecordVersion:   3,
			Condition:         10,
			LastWriteVersion:  7,
		},
		UpdateWorkflowNewEvents: []*p.InternalAppendHistoryNodesRequest{{
			BranchInfo: &persistencespb.HistoryBranch{
				TreeId:   primitives.NewUUID().String(),
				BranchId: primitives.NewUUID().String(),
			},
			TreeInfo: &commonpb.DataBlob{},
			Node:     p.InternalHistoryNode{NodeID: 10, Events: &commonpb.DataBlob{}},
			ShardID:  1,
		}},
	}
}

func newFakePipelineDB(request *p.InternalUpdateWorkflowExecutionRequest) *fakePipelineDB {
	return &fakePipelineDB{
		pipelined:  &fakeUpdateTx{request: request},
		sequential: &fakeUpdateTx{request: request},
	}
}

// updateWithFakes runs the update on a store whose DB is wrapped in a DbConn,
// as Factory.NewExecutionStore wraps it.
func updateWithFakes(db *fakePipelineDB, request *p.InternalUpdateWorkflowExecutionRequest) error {
	store, err := NewSQLExecutionStore(&DbConn{DB: db}, log.NewNoopLogger(), serialization.NewSerializer())
	if err != nil {
		return err
	}
	return store.UpdateWorkflowExecution(context.Background(), request)
}

func (d *fakePipelineDB) BeginPipelinedTx(context.Context) (sqlplugin.PipelinedTx, error) {
	if d.beginPipelinedErr != nil {
		return nil, d.beginPipelinedErr
	}
	d.pipelined.begun = true
	return d.pipelined, nil
}

func (d *fakePipelineDB) BeginTx(context.Context) (sqlplugin.Tx, error) {
	d.sequential.begun = true
	return d.sequential, nil
}

func (d *fakePipelineDB) InsertIntoHistoryNode(context.Context, *sqlplugin.HistoryNodeRow) (sql.Result, error) {
	d.historyNodes++
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) InsertIntoHistoryNode(context.Context, *sqlplugin.HistoryNodeRow) (sql.Result, error) {
	t.historyNodes++
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) InsertIntoHistoryTree(context.Context, *sqlplugin.HistoryTreeRow) (sql.Result, error) {
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) ReadLockShards(context.Context, sqlplugin.ShardsFilter) (int64, error) {
	return t.request.RangeID, nil
}

func (t *fakeUpdateTx) ReadLockShardsWithRangeID(context.Context, sqlplugin.ShardsFilter, int64) error {
	return nil
}

func (t *fakeUpdateTx) LockCurrentExecutions(context.Context, sqlplugin.CurrentExecutionsFilter) (*sqlplugin.CurrentExecutionsRow, error) {
	if t.request.Mode == p.UpdateWorkflowModeBypassCurrent {
		return nil, sql.ErrNoRows
	}
	return &sqlplugin.CurrentExecutionsRow{
		RunID: primitives.MustParseUUID(t.request.UpdateWorkflowMutation.ExecutionState.RunId),
	}, nil
}

func (t *fakeUpdateTx) LockCurrentExecutionsWithRunID(_ context.Context, filter sqlplugin.CurrentExecutionsFilter) error {
	t.lockedCurrent = &filter
	return nil
}

func (t *fakeUpdateTx) UpdateCurrentExecutionsIfChanged(_ context.Context, row *sqlplugin.CurrentExecutionsRow) error {
	t.updatedCurrent = row
	return nil
}

func (t *fakeUpdateTx) UpdateCurrentExecutions(context.Context, *sqlplugin.CurrentExecutionsRow) (sql.Result, error) {
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) InsertIntoExecutions(context.Context, *sqlplugin.ExecutionsRow) (sql.Result, error) {
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) UpdateExecutionsWithCondition(context.Context, *sqlplugin.ExecutionsUpdate) (sql.Result, error) {
	if t.updateExecutionsErr != nil {
		return nil, t.updateExecutionsErr
	}
	return driver.RowsAffected(1), nil
}

func (t *fakeUpdateTx) Commit() error {
	if t.commitErr != nil {
		return t.commitErr
	}
	t.committed = true
	return nil
}

func (t *fakeUpdateTx) Rollback() error {
	return nil
}
