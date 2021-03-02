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

package gocql

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/service/config"
)

var _ Session = (*session)(nil)

const (
	sessionRefreshMinInternal = 5 * time.Second
)

type (
	session struct {
		status       int32
		config       config.Cassandra
		resolver     resolver.ServiceResolver
		atomic.Value // *gocql.Session

		sync.Mutex
		sessionInitTime time.Time
	}
)

func NewSession(
	config config.Cassandra,
	resolver resolver.ServiceResolver,
) (*session, error) {

	gocqlSession, err := initSession(config, resolver)
	if err != nil {
		return nil, err
	}

	session := &session{
		status:          common.DaemonStatusStarted,
		config:          config,
		sessionInitTime: time.Now().UTC(),
	}
	session.Value.Store(gocqlSession)
	return session, nil
}

func (s *session) refresh() error {
	s.Lock()
	defer s.Unlock()

	if atomic.LoadInt32(&s.status) != common.DaemonStatusStarted {
		return nil
	}

	if time.Now().UTC().Sub(s.sessionInitTime) < sessionRefreshMinInternal {
		return nil
	}

	gocqlSession, err := initSession(s.config, s.resolver)
	if err != nil {
		return err
	}

	s.sessionInitTime = time.Now().UTC()
	s.Value.Load().(*gocql.Session).Close()
	s.Value.Store(gocqlSession)
	return nil
}

func initSession(
	config config.Cassandra,
	resolver resolver.ServiceResolver,
) (*gocql.Session, error) {
	cluster, err := NewCassandraCluster(config, resolver)
	if err != nil {
		return nil, err
	}
	cluster.ProtoVersion = 4
	cluster.Consistency = config.Consistency.GetConsistency()
	cluster.SerialConsistency = config.Consistency.GetSerialConsistency()
	cluster.Timeout = 10 * time.Second
	return cluster.CreateSession()
}

func (s *session) Query(
	stmt string,
	values ...interface{},
) Query {
	q := s.Value.Load().(*gocql.Session).Query(stmt, values...)
	if q == nil {
		return nil
	}
	return &query{
		session:    s,
		gocqlQuery: q,
	}
}

func (s *session) NewBatch(
	batchType BatchType,
) Batch {
	b := s.Value.Load().(*gocql.Session).NewBatch(mustConvertBatchType(batchType))
	if b == nil {
		return nil
	}
	return &batch{
		session:    s,
		gocqlBatch: b,
	}
}

func (s *session) ExecuteBatch(
	b Batch,
) (retError error) {
	defer func() { s.handleError(retError) }()

	return s.Value.Load().(*gocql.Session).ExecuteBatch(b.(*batch).gocqlBatch)
}

func (s *session) MapExecuteBatchCAS(
	b Batch,
	previous map[string]interface{},
) (_ bool, _ Iter, retError error) {
	defer func() { s.handleError(retError) }()

	applied, iter, err := s.Value.Load().(*gocql.Session).MapExecuteBatchCAS(b.(*batch).gocqlBatch, previous)
	if iter == nil {
		return applied, nil, err
	}
	return applied, iter, err
}

func (s *session) Close() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	s.Value.Load().(*gocql.Session).Close()
}

func (s *session) handleError(
	err error,
) {
	switch err {
	case gocql.ErrNoConnections:
		_ = s.refresh()
	default:
		// noop
	}
}
