package cassandra

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protorequire"
)

type (
	cassandraErrorsSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions
	}
)

func TestCassandraErrorsSuite(t *testing.T) {
	s := new(cassandraErrorsSuite)
	suite.Run(t, s)
}

func (s *cassandraErrorsSuite) SetupSuite() {
}

func (s *cassandraErrorsSuite) TearDownSuite() {

}

func (s *cassandraErrorsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
}

func (s *cassandraErrorsSuite) TearDownTest() {

}

func (s *cassandraErrorsSuite) TestSortErrors_Sorted() {
	shardOwnershipLostErr := &p.ShardOwnershipLostError{}
	currentWorkflowErr := &p.CurrentWorkflowConditionFailedError{}
	workflowErr := &p.WorkflowConditionFailedError{}
	genericErr := &p.ConditionFailedError{}
	randomErr := errors.New("random error")

	expectedErrors := []error{
		shardOwnershipLostErr,
		currentWorkflowErr,
		workflowErr,
		genericErr,
		randomErr,
	}

	errorsCaseSorted := []error{
		shardOwnershipLostErr,
		currentWorkflowErr,
		workflowErr,
		genericErr,
		randomErr,
	}
	s.Equal(expectedErrors, sortErrors(errorsCaseSorted))
}

func (s *cassandraErrorsSuite) TestSortErrors_ReverseSorted() {
	shardOwnershipLostErr := &p.ShardOwnershipLostError{}
	currentWorkflowErr := &p.CurrentWorkflowConditionFailedError{}
	workflowErr := &p.WorkflowConditionFailedError{}
	genericErr := &p.ConditionFailedError{}
	randomErr := errors.New("random error")

	expectedErrors := []error{
		shardOwnershipLostErr,
		currentWorkflowErr,
		workflowErr,
		genericErr,
		randomErr,
	}

	errorsCaseReverseSorted := []error{
		randomErr,
		genericErr,
		workflowErr,
		currentWorkflowErr,
		shardOwnershipLostErr,
	}
	s.Equal(expectedErrors, sortErrors(errorsCaseReverseSorted))
}

func (s *cassandraErrorsSuite) TestSortErrors_Random() {
	shardOwnershipLostErr := &p.ShardOwnershipLostError{}
	currentWorkflowErr := &p.CurrentWorkflowConditionFailedError{}
	workflowErr := &p.WorkflowConditionFailedError{}
	genericErr := &p.ConditionFailedError{}
	randomErr := errors.New("random error")

	expectedErrors := []error{
		shardOwnershipLostErr,
		currentWorkflowErr,
		workflowErr,
		genericErr,
		randomErr,
	}

	errorsCaseShuffled := []error{
		randomErr,
		genericErr,
		workflowErr,
		currentWorkflowErr,
		shardOwnershipLostErr,
	}
	rand.Shuffle(len(errorsCaseShuffled), func(i int, j int) {
		errorsCaseShuffled[i], errorsCaseShuffled[j] = errorsCaseShuffled[j], errorsCaseShuffled[i]
	})
	s.Equal(expectedErrors, sortErrors(errorsCaseShuffled))
}

func (s *cassandraErrorsSuite) TestSortErrors_One() {
	shardOwnershipLostErr := &p.ShardOwnershipLostError{}
	currentWorkflowErr := &p.CurrentWorkflowConditionFailedError{}
	workflowErr := &p.WorkflowConditionFailedError{}
	genericErr := &p.ConditionFailedError{}
	randomErr := errors.New("random error")

	s.Equal([]error{shardOwnershipLostErr}, sortErrors([]error{shardOwnershipLostErr}))
	s.Equal([]error{currentWorkflowErr}, sortErrors([]error{currentWorkflowErr}))
	s.Equal([]error{workflowErr}, sortErrors([]error{workflowErr}))
	s.Equal([]error{genericErr}, sortErrors([]error{genericErr}))
	s.Equal([]error{randomErr}, sortErrors([]error{randomErr}))
}

func (s *cassandraErrorsSuite) TestExtractShardOwnershipLostError_Failed() {
	rangeID := int64(1234)

	err := extractShardOwnershipLostError(map[string]interface{}{}, rand.Int31(), rangeID)
	s.NoError(err)

	t := rowTypeExecution
	err = extractShardOwnershipLostError(map[string]interface{}{
		"type":     &t,
		"range_id": rangeID,
	}, rand.Int31(), rangeID)
	s.NoError(err)

	t = rowTypeShard
	err = extractShardOwnershipLostError(map[string]interface{}{
		"type":     &t,
		"range_id": rangeID,
	}, rand.Int31(), rangeID)
	s.NoError(err)
}

func (s *cassandraErrorsSuite) TestExtractShardOwnershipLostError_Success() {
	rangeID := int64(1234)
	t := rowTypeShard
	record := map[string]interface{}{
		"type":     &t,
		"range_id": rangeID,
	}

	err := extractShardOwnershipLostError(record, rand.Int31(), rangeID+1)
	s.IsType(&p.ShardOwnershipLostError{}, err)
}

func (s *cassandraErrorsSuite) TestExtractCurrentWorkflowConflictError_Failed() {
	runID, _ := uuid.Parse(permanentRunID)
	currentRunID := uuid.New()

	err := extractCurrentWorkflowConflictError(map[string]interface{}{}, uuid.New().String())
	s.NoError(err)

	t := rowTypeShard
	err = extractCurrentWorkflowConflictError(map[string]interface{}{
		"type":           &t,
		"run_id":         gocql.UUID(runID),
		"current_run_id": gocql.UUID(currentRunID),
	}, uuid.New().String())
	s.NoError(err)

	t = rowTypeExecution
	err = extractCurrentWorkflowConflictError(map[string]interface{}{
		"type":           &t,
		"run_id":         gocql.UUID([16]byte{}),
		"current_run_id": gocql.UUID(currentRunID),
	}, uuid.New().String())
	s.NoError(err)

	t = rowTypeExecution
	err = extractCurrentWorkflowConflictError(map[string]interface{}{
		"type":           &t,
		"run_id":         gocql.UUID(runID),
		"current_run_id": gocql.UUID(currentRunID),
	}, currentRunID.String())
	s.NoError(err)
}

func (s *cassandraErrorsSuite) TestExtractCurrentWorkflowConflictError_Success() {
	requestID := uuid.New()
	runID, _ := uuid.Parse(permanentRunID)
	currentRunID := uuid.New()
	startTime := time.Now().UTC()
	workflowState := &persistencespb.WorkflowExecutionState{
		CreateRequestId: requestID.String(),
		RunId:           currentRunID.String(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime:       timestamp.TimePtr(startTime),
		RequestIds: map[string]*persistencespb.RequestIDInfo{
			requestID.String(): {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				EventId:   common.FirstEventID,
			},
			uuid.NewString(): {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				EventId:   common.BufferedEventID,
			},
		},
	}
	blob, err := serialization.WorkflowExecutionStateToBlob(workflowState)
	lastWriteVersion := rand.Int63()
	s.NoError(err)
	t := rowTypeExecution
	record := map[string]interface{}{
		"type":                        &t,
		"run_id":                      gocql.UUID(runID),
		"current_run_id":              gocql.UUID(currentRunID),
		"execution_state":             blob.Data,
		"execution_state_encoding":    blob.EncodingType.String(),
		"workflow_last_write_version": lastWriteVersion,
	}

	err = extractCurrentWorkflowConflictError(record, uuid.New().String())
	if err, ok := err.(*p.CurrentWorkflowConditionFailedError); ok {
		err.Msg = ""
	}
	s.DeepEqual(
		&p.CurrentWorkflowConditionFailedError{
			Msg:              "",
			RequestIDs:       workflowState.RequestIds,
			RunID:            workflowState.RunId,
			State:            workflowState.State,
			Status:           workflowState.Status,
			LastWriteVersion: lastWriteVersion,
			StartTime:        &startTime,
		},
		err,
	)
}

func (s *cassandraErrorsSuite) TestExtractWorkflowConflictError_Failed() {
	runID := uuid.New()
	dbVersion := rand.Int63() + 1

	err := extractWorkflowConflictError(map[string]interface{}{}, runID.String(), dbVersion, rand.Int63())
	s.NoError(err)

	t := rowTypeShard
	err = extractWorkflowConflictError(map[string]interface{}{
		"type":              &t,
		"run_id":            gocql.UUID(runID),
		"db_record_version": dbVersion,
	}, runID.String(), dbVersion+1, rand.Int63())
	s.NoError(err)

	t = rowTypeExecution
	err = extractWorkflowConflictError(map[string]interface{}{
		"type":              &t,
		"run_id":            gocql.UUID([16]byte{}),
		"db_record_version": dbVersion,
	}, runID.String(), dbVersion+1, rand.Int63())
	s.NoError(err)

	t = rowTypeExecution
	err = extractWorkflowConflictError(map[string]interface{}{
		"type":              &t,
		"run_id":            gocql.UUID(runID),
		"db_record_version": dbVersion,
	}, runID.String(), dbVersion, rand.Int63())
	s.NoError(err)
}

func (s *cassandraErrorsSuite) TestExtractWorkflowConflictError_Success() {
	runID := uuid.New()
	dbVersion := rand.Int63() + 1
	t := rowTypeExecution
	record := map[string]interface{}{
		"type":              &t,
		"run_id":            gocql.UUID(runID),
		"db_record_version": dbVersion,
	}

	err := extractWorkflowConflictError(record, runID.String(), dbVersion+1, rand.Int63())
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

// TODO remove this block once DB version comparison is the default
func (s *cassandraErrorsSuite) TestExtractWorkflowConflictError_Failed_NextEventID() {
	runID := uuid.New()
	nextEventID := rand.Int63()

	err := extractWorkflowConflictError(map[string]interface{}{}, runID.String(), 0, nextEventID)
	s.NoError(err)

	t := rowTypeShard
	err = extractWorkflowConflictError(map[string]interface{}{
		"type":          &t,
		"run_id":        gocql.UUID(runID),
		"next_event_id": nextEventID + 1,
	}, runID.String(), 0, nextEventID)
	s.NoError(err)

	t = rowTypeExecution
	err = extractWorkflowConflictError(map[string]interface{}{
		"type":          &t,
		"run_id":        gocql.UUID([16]byte{}),
		"next_event_id": nextEventID + 1,
	}, runID.String(), 0, nextEventID)
	s.NoError(err)

	t = rowTypeExecution
	err = extractWorkflowConflictError(map[string]interface{}{
		"type":          &t,
		"run_id":        gocql.UUID(runID),
		"next_event_id": nextEventID,
	}, runID.String(), 0, nextEventID)
	s.NoError(err)
}

// TODO remove this block once DB version comparison is the default
func (s *cassandraErrorsSuite) TestExtractWorkflowConflictError_Success_NextEventID() {
	runID := uuid.New()
	nextEventID := int64(1234)
	t := rowTypeExecution
	record := map[string]interface{}{
		"type":          &t,
		"run_id":        gocql.UUID(runID),
		"next_event_id": nextEventID,
	}

	err := extractWorkflowConflictError(record, runID.String(), 0, nextEventID+1)
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}
