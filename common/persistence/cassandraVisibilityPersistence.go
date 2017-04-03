package persistence

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

// Fixed domain values for now
const (
	domainPartition = 0
)

const (
	templateCreateWorkflowExecutionStarted = `INSERT INTO open_executions (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, workflow_type_name) ` +
		`VALUES (?, ?, ?, ?, ?, ?)`

	templateDeleteWorkflowExecutionStarted = `DELETE FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition = ? ` +
		`AND workflow_id = ? ` +
		`AND run_id = ? `

	templateCreateWorkflowExecutionClosed = `INSERT INTO closed_executions (` +
		`domain_id, domain_partition, workflow_id, run_id, start_time, close_time, workflow_type_name) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?) using TTL ?`

	templateGetOpenWorkflowExecutions = `SELECT workflow_id, run_id, start_time, workflow_type_name ` +
		`FROM open_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) `

	templateGetClosedWorkflowExecutions = `SELECT workflow_id, run_id, start_time, close_time, workflow_type_name ` +
		`FROM closed_executions ` +
		`WHERE domain_id = ? ` +
		`AND domain_partition IN (?) `
)

type (
	cassandraVisibilityPersistence struct {
		session      *gocql.Session
		lowConslevel gocql.Consistency
		logger       bark.Logger
	}
)

// NewCassandraVisibilityPersistence is used to create an instance of VisibilityManager implementation
func NewCassandraVisibilityPersistence(
	hosts string, dc string, keyspace string, logger bark.Logger) (VisibilityManager, error) {
	cluster := common.NewCassandraCluster(hosts, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraVisibilityPersistence{session: session, lowConslevel: gocql.One, logger: logger}, nil
}

func (v *cassandraVisibilityPersistence) RecordWorkflowExecutionStarted(
	request *RecordWorkflowExecutionStartedRequest) error {
	query := v.session.Query(templateCreateWorkflowExecutionStarted,
		request.DomainUUID,
		domainPartition,
		request.Execution.GetWorkflowId(),
		request.Execution.GetRunId(),
		request.StartTime,
		request.WorkflowTypeName,
	)
	err := query.Exec()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RecordWorkflowExecutionStarted operation failed. Error: %v", err),
		}
	}

	return nil
}

func (v *cassandraVisibilityPersistence) RecordWorkflowExecutionClosed(
	request *RecordWorkflowExecutionClosedRequest) error {
	batch := v.session.NewBatch(gocql.LoggedBatch)

	// First, remove execution from the open table
	batch.Query(templateDeleteWorkflowExecutionStarted,
		request.DomainUUID,
		domainPartition,
		request.Execution.GetWorkflowId(),
		request.Execution.GetRunId(),
	)

	// Next, add a row in the closed table. This row is kepy for defaultDeleteTTLSeconds
	batch.Query(templateCreateWorkflowExecutionClosed,
		request.DomainUUID,
		domainPartition,
		request.Execution.GetWorkflowId(),
		request.Execution.GetRunId(),
		request.StartTime,
		request.CloseTime,
		request.WorkflowTypeName,
		defaultDeleteTTLSeconds,
	)
	err := v.session.ExecuteBatch(batch)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RecordWorkflowExecutionClosed operation failed. Error: %v", err),
		}
	}
	return nil
}

func (v *cassandraVisibilityPersistence) ListOpenWorkflowExecutions(
	request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetOpenWorkflowExecutions, request.DomainUUID, domainPartition)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListOpenWorkflowExecutions operation failed.  Not able to create query iterator.",
		}
	}

	response := &ListWorkflowExecutionsResponse{}
	response.Executions = make([]*WorkflowExecutionRecord, 0)
	rec := make(map[string]interface{})
	for iter.MapScan(rec) {
		wfexecution := createWorkflowExecutionRecord(rec)
		response.Executions = append(response.Executions, wfexecution)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListOpenWorkflowExecutions operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (v *cassandraVisibilityPersistence) ListClosedWorkflowExecutions(
	request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	query := v.session.Query(templateGetClosedWorkflowExecutions, request.DomainUUID, domainPartition)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		// TODO: should return a bad request error if the token is invalid
		return nil, &workflow.InternalServiceError{
			Message: "ListOpenWorkflowExecutions operation failed.  Not able to create query iterator.",
		}
	}

	response := &ListWorkflowExecutionsResponse{}
	response.Executions = make([]*WorkflowExecutionRecord, 0)
	rec := make(map[string]interface{})
	for iter.MapScan(rec) {
		wfexecution := createWorkflowExecutionRecord(rec)
		response.Executions = append(response.Executions, wfexecution)
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ListOpenWorkflowExecutions operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func createWorkflowExecutionRecord(result map[string]interface{}) *WorkflowExecutionRecord {
	record := &WorkflowExecutionRecord{}
	for k, v := range result {
		switch k {
		case "workflow_id":
			record.Execution.WorkflowId = common.StringPtr(v.(string))
		case "run_id":
			record.Execution.RunId = common.StringPtr(v.(gocql.UUID).String())
		case "workflow_type_name":
			record.WorkflowTypeName = v.(string)
		case "start_time":
			record.StartTime = v.(time.Time)
		case "close_time":
			record.CloseTime = v.(time.Time)
		default:
			// Unknown field, could happen due to schema update
		}
	}

	return record
}
