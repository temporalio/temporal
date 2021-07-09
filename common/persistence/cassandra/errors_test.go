// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cassandra

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	p "go.temporal.io/server/common/persistence"
)

type (
	cassandraErrorsSuite struct {
		suite.Suite
		*require.Assertions
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
}

func (s *cassandraErrorsSuite) TearDownTest() {

}

func (s *cassandraErrorsSuite) TestSortErrors_Sorted() {
	shardOwnershipLostErr := &p.ShardOwnershipLostError{}
	concurrentWorkflowErr := &p.CurrentWorkflowConditionFailedError{}
	workflowConditionFailedErr := &p.ConditionFailedError{}
	randomErr := errors.New("random error")

	expectedErrors := []error{
		shardOwnershipLostErr,
		concurrentWorkflowErr,
		workflowConditionFailedErr,
		randomErr,
	}

	errorsCaseSorted := []error{
		shardOwnershipLostErr,
		concurrentWorkflowErr,
		workflowConditionFailedErr,
		randomErr,
	}
	s.Equal(expectedErrors, sortErrors(errorsCaseSorted))
}

func (s *cassandraErrorsSuite) TestSortErrors_ReverseSorted() {
	shardOwnershipLostErr := &p.ShardOwnershipLostError{}
	concurrentWorkflowErr := &p.CurrentWorkflowConditionFailedError{}
	workflowConditionFailedErr := &p.ConditionFailedError{}
	randomErr := errors.New("random error")

	expectedErrors := []error{
		shardOwnershipLostErr,
		concurrentWorkflowErr,
		workflowConditionFailedErr,
		randomErr,
	}

	errorsCaseReverseSorted := []error{
		randomErr,
		workflowConditionFailedErr,
		concurrentWorkflowErr,
		shardOwnershipLostErr,
	}
	s.Equal(expectedErrors, sortErrors(errorsCaseReverseSorted))
}

func (s *cassandraErrorsSuite) TestSortErrors_Random() {
	shardOwnershipLostErr := &p.ShardOwnershipLostError{}
	concurrentWorkflowErr := &p.CurrentWorkflowConditionFailedError{}
	workflowConditionFailedErr := &p.ConditionFailedError{}
	randomErr := errors.New("random error")

	expectedErrors := []error{
		shardOwnershipLostErr,
		concurrentWorkflowErr,
		workflowConditionFailedErr,
		randomErr,
	}

	errorsCaseShuffled := []error{
		randomErr,
		workflowConditionFailedErr,
		concurrentWorkflowErr,
		shardOwnershipLostErr,
	}
	rand.Shuffle(len(errorsCaseShuffled), func(i int, j int) {
		errorsCaseShuffled[i], errorsCaseShuffled[j] = errorsCaseShuffled[j], errorsCaseShuffled[i]
	})
	s.Equal(expectedErrors, sortErrors(errorsCaseShuffled))
}

func (s *cassandraErrorsSuite) TestSortErrors_One() {
	shardOwnershipLostErr := &p.ShardOwnershipLostError{}
	concurrentWorkflowErr := &p.CurrentWorkflowConditionFailedError{}
	workflowConditionFailedErr := &p.ConditionFailedError{}
	randomErr := errors.New("random error")

	s.Equal([]error{shardOwnershipLostErr}, sortErrors([]error{shardOwnershipLostErr}))
	s.Equal([]error{concurrentWorkflowErr}, sortErrors([]error{concurrentWorkflowErr}))
	s.Equal([]error{workflowConditionFailedErr}, sortErrors([]error{workflowConditionFailedErr}))
	s.Equal([]error{randomErr}, sortErrors([]error{randomErr}))
}

func (s *cassandraErrorsSuite) TestExtractShardOwnershipLostError_Failed() {
	rangeID := int64(1234)

	err := extractShardOwnershipLostError(map[string]interface{}{}, rand.Int31(), rangeID)
	s.NoError(err)

	err = extractShardOwnershipLostError(map[string]interface{}{
		"type":     rowTypeExecution,
		"range_id": rangeID,
	}, rand.Int31(), rangeID)
	s.NoError(err)

	err = extractShardOwnershipLostError(map[string]interface{}{
		"type":     rowTypeShard,
		"range_id": rangeID,
	}, rand.Int31(), rangeID)
	s.NoError(err)
}

func (s *cassandraErrorsSuite) TestExtractShardOwnershipLostError_Success() {
	rangeID := int64(1234)
	record := map[string]interface{}{
		"type":     rowTypeShard,
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

	err = extractCurrentWorkflowConflictError(map[string]interface{}{
		"type":           rowTypeShard,
		"run_id":         gocql.UUID(runID),
		"current_run_id": gocql.UUID(currentRunID),
	}, uuid.New().String())
	s.NoError(err)

	err = extractCurrentWorkflowConflictError(map[string]interface{}{
		"type":           rowTypeExecution,
		"run_id":         gocql.UUID([16]byte{}),
		"current_run_id": gocql.UUID(currentRunID),
	}, uuid.New().String())
	s.NoError(err)

	err = extractCurrentWorkflowConflictError(map[string]interface{}{
		"type":           rowTypeExecution,
		"run_id":         gocql.UUID(runID),
		"current_run_id": gocql.UUID(currentRunID),
	}, currentRunID.String())
	s.NoError(err)
}

func (s *cassandraErrorsSuite) TestExtractCurrentWorkflowConflictError_Success() {
	runID, _ := uuid.Parse(permanentRunID)
	currentRunID := uuid.New()
	record := map[string]interface{}{
		"type":           rowTypeExecution,
		"run_id":         gocql.UUID(runID),
		"current_run_id": gocql.UUID(currentRunID),
	}

	err := extractCurrentWorkflowConflictError(record, uuid.New().String())
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)
}

func (s *cassandraErrorsSuite) TestExtractWorkflowVersionConflictError_Failed() {
	runID := uuid.New()
	dbVersion := rand.Int63() + 1

	err := extractWorkflowVersionConflictError(map[string]interface{}{}, runID.String(), dbVersion, rand.Int63())
	s.NoError(err)

	err = extractWorkflowVersionConflictError(map[string]interface{}{
		"type":       rowTypeShard,
		"run_id":     gocql.UUID(runID),
		"db_version": dbVersion,
	}, runID.String(), dbVersion+1, rand.Int63())
	s.NoError(err)

	err = extractWorkflowVersionConflictError(map[string]interface{}{
		"type":       rowTypeExecution,
		"run_id":     gocql.UUID([16]byte{}),
		"db_version": dbVersion,
	}, runID.String(), dbVersion+1, rand.Int63())
	s.NoError(err)

	err = extractWorkflowVersionConflictError(map[string]interface{}{
		"type":       rowTypeExecution,
		"run_id":     gocql.UUID(runID),
		"db_version": dbVersion,
	}, runID.String(), dbVersion, rand.Int63())
	s.NoError(err)
}

func (s *cassandraErrorsSuite) TestExtractWorkflowVersionConflictError_Success() {
	runID := uuid.New()
	dbVersion := rand.Int63() + 1
	record := map[string]interface{}{
		"type":       rowTypeExecution,
		"run_id":     gocql.UUID(runID),
		"db_version": dbVersion,
	}

	err := extractWorkflowVersionConflictError(record, runID.String(), dbVersion+1, rand.Int63())
	s.IsType(&p.ConditionFailedError{}, err)
}

// TODO remove this block once DB version comparison is the default
func (s *cassandraErrorsSuite) TestExtractWorkflowVersionConflictError_Failed_NextEventID() {
	runID := uuid.New()
	nextEventID := rand.Int63()

	err := extractWorkflowVersionConflictError(map[string]interface{}{}, runID.String(), 0, nextEventID)
	s.NoError(err)

	err = extractWorkflowVersionConflictError(map[string]interface{}{
		"type":          rowTypeShard,
		"run_id":        gocql.UUID(runID),
		"next_event_id": nextEventID + 1,
	}, runID.String(), 0, nextEventID)
	s.NoError(err)

	err = extractWorkflowVersionConflictError(map[string]interface{}{
		"type":          rowTypeExecution,
		"run_id":        gocql.UUID([16]byte{}),
		"next_event_id": nextEventID + 1,
	}, runID.String(), 0, nextEventID)
	s.NoError(err)

	err = extractWorkflowVersionConflictError(map[string]interface{}{
		"type":          rowTypeExecution,
		"run_id":        gocql.UUID(runID),
		"next_event_id": nextEventID,
	}, runID.String(), 0, nextEventID)
	s.NoError(err)
}

// TODO remove this block once DB version comparison is the default
func (s *cassandraErrorsSuite) TestExtractWorkflowVersionConflictError_Success_NextEventID() {
	runID := uuid.New()
	nextEventID := int64(1234)
	record := map[string]interface{}{
		"type":          rowTypeExecution,
		"run_id":        gocql.UUID(runID),
		"next_event_id": nextEventID,
	}

	err := extractWorkflowVersionConflictError(record, runID.String(), 0, nextEventID+1)
	s.IsType(&p.ConditionFailedError{}, err)
}
