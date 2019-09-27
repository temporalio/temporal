// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"time"

	"github.com/gocql/gocql"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

const (
	firstMessageID = 0
)

const (
	templateEnqueueMessageQuery   = `INSERT INTO queue (queue_type, message_id, message_payload) VALUES(?, ?, ?) IF NOT EXISTS`
	templateGetLastMessageIDQuery = `SELECT message_id FROM queue WHERE queue_type=? ORDER BY message_id DESC LIMIT 1`
	templateGetMessagesQuery      = `SELECT message_id, message_payload FROM queue WHERE queue_type = ? and message_id > ? LIMIT ?`
)

type (
	cassandraQueue struct {
		queueType int
		logger    log.Logger
		cassandraStore
	}
)

func newQueue(
	cfg config.Cassandra,
	logger log.Logger,
	queueType int,
) (persistence.Queue, error) {
	cluster := NewCassandraCluster(cfg.Hosts, cfg.Port, cfg.User, cfg.Password, cfg.Datacenter)
	cluster.Keyspace = cfg.Keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	return &cassandraQueue{
		cassandraStore: cassandraStore{session: session, logger: logger},
		logger:         logger,
		queueType:      queueType,
	}, nil
}

func (q *cassandraQueue) EnqueueMessage(
	messagePayload []byte,
) error {
	nextMessageID, err := q.getNextMessageID()
	if err != nil {
		return err
	}

	return q.tryEnqueue(nextMessageID, messagePayload)
}

func (q *cassandraQueue) tryEnqueue(
	messageID int,
	messagePayload []byte,
) error {
	query := q.session.Query(templateEnqueueMessageQuery, q.queueType, messageID, messagePayload)
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("Failed to enqueue message. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to enqueue message. Error: %v", err),
		}
	}

	if !applied {
		return &persistence.ConditionFailedError{Msg: fmt.Sprintf("message ID %v exists in queue", previous["message_id"])}
	}

	return nil
}

func (q *cassandraQueue) getNextMessageID() (int, error) {
	query := q.session.Query(templateGetLastMessageIDQuery, q.queueType)

	result := make(map[string]interface{})
	err := query.MapScan(result)
	if err != nil {
		if err == gocql.ErrNotFound {
			return firstMessageID, nil
		} else if isThrottlingError(err) {
			return 0, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("Failed to get last message ID for queue %v. Error: %v", q.queueType, err),
			}
		}

		return 0, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get last message ID for queue %v. Error: %v", q.queueType, err),
		}
	}

	return result["message_id"].(int) + 1, nil
}

func (q *cassandraQueue) DequeueMessages(
	lastMessageID int,
	maxCount int,
) ([]*persistence.QueueMessage, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could loose task
	query := q.session.Query(templateGetMessagesQuery,
		q.queueType,
		lastMessageID,
		maxCount,
	)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "DequeueMessages operation failed. Not able to create query iterator.",
		}
	}

	var result []*persistence.QueueMessage
	message := make(map[string]interface{})
	for iter.MapScan(message) {
		payload := getMessagePayload(message)
		id := getMessageID(message)
		result = append(result, &persistence.QueueMessage{ID: id, Payload: payload})
		message = make(map[string]interface{})
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("DequeueMessages operation failed. Error: %v", err),
		}
	}

	return result, nil
}

func (q *cassandraQueue) Close() error {
	if q.session != nil {
		q.session.Close()
	}

	return nil
}

func getMessagePayload(message map[string]interface{}) []byte {
	return message["message_payload"].([]byte)
}

func getMessageID(message map[string]interface{}) int {
	return message["message_id"].(int)
}
