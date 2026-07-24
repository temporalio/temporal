package gocql

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ Session = (*session)(nil)

const (
	sessionRefreshMinInternal = 5 * time.Second
)

const (
	refreshThrottleTagValue = "throttle"
	refreshErrorTagValue    = "error"
	missingPeersV2Table     = "unconfigured table peers_v2"
)

type (
	session struct {
		status               int32
		newClusterConfigFunc func() (*gocql.ClusterConfig, error)
		atomic.Value         // *gocql.Session
		logger               log.Logger

		sync.Mutex
		sessionInitTime time.Time
		metricsHandler  metrics.Handler
	}
)

func NewSession(
	newClusterConfigFunc func() (*gocql.ClusterConfig, error),
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*session, error) {

	gocqlSession, err := initSession(logger, newClusterConfigFunc, metricsHandler)
	if err != nil {
		return nil, err
	}

	session := &session{
		status:               common.DaemonStatusStarted,
		newClusterConfigFunc: newClusterConfigFunc,
		logger:               logger,
		metricsHandler:       metricsHandler,

		sessionInitTime: time.Now().UTC(),
	}
	session.Value.Store(gocqlSession)
	return session, nil
}

func (s *session) refresh() {
	if atomic.LoadInt32(&s.status) != common.DaemonStatusStarted {
		return
	}

	s.Lock()
	defer s.Unlock()

	if time.Now().UTC().Sub(s.sessionInitTime) < sessionRefreshMinInternal {
		s.logger.Warn("gocql wrapper: did not refresh gocql session because the last refresh was too close",
			tag.Duration("min_refresh_interval_seconds", sessionRefreshMinInternal))
		handler := s.metricsHandler.WithTags(metrics.FailureTag(refreshThrottleTagValue))
		metrics.CassandraSessionRefreshFailures.With(handler).Record(1)
		return
	}

	newSession, err := initSession(s.logger, s.newClusterConfigFunc, s.metricsHandler)
	if err != nil {
		s.logger.Error("gocql wrapper: unable to refresh gocql session", tag.Error(err))
		handler := s.metricsHandler.WithTags(metrics.FailureTag(refreshErrorTagValue))
		metrics.CassandraSessionRefreshFailures.With(handler).Record(1)
		return
	}

	s.sessionInitTime = time.Now().UTC()
	oldSession := s.Value.Load().(*gocql.Session)
	s.Value.Store(newSession)
	go oldSession.Close()
	s.logger.Warn("gocql wrapper: successfully refreshed gocql session")
}

func initSession(
	logger log.Logger,
	newClusterConfigFunc func() (*gocql.ClusterConfig, error),
	metricsHandler metrics.Handler,
) (gs *gocql.Session, retErr error) {
	defer log.CapturePanic(logger, &retErr)
	cluster, err := newClusterConfigFunc()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	defer func() {
		metrics.CassandraInitSessionLatency.With(metricsHandler).Record(time.Since(start))
	}()
	session, err := cluster.CreateSession()
	if err == nil {
		return session, nil
	}
	if !shouldRetryWithoutInitialHostLookup(cluster, err) {
		return nil, err
	}
	logger.Warn("gocql wrapper: retrying session initialization with initial host lookup disabled", tag.Error(err))
	retryCluster, retryErr := newClusterConfigFunc()
	if retryErr != nil {
		return nil, retryErr
	}
	retryCluster.DisableInitialHostLookup = true
	return retryCluster.CreateSession()
}

func shouldRetryWithoutInitialHostLookup(cluster *gocql.ClusterConfig, err error) bool {
	return !cluster.DisableInitialHostLookup && isMissingPeersV2TableError(err)
}

func isMissingPeersV2TableError(err error) bool {
	return err != nil && strings.Contains(err.Error(), missingPeersV2Table)
}

func (s *session) Query(
	stmt string,
	values ...any,
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
) *Batch {
	b := s.Value.Load().(*gocql.Session).NewBatch(mustConvertBatchType(batchType))
	if b == nil {
		return nil
	}
	return &Batch{
		session:    s,
		gocqlBatch: b,
	}
}

func (s *session) ExecuteBatch(
	b *Batch,
) (retError error) {
	defer func() { s.handleError(retError) }()

	return s.Value.Load().(*gocql.Session).ExecuteBatch(b.gocqlBatch)
}

func (s *session) MapExecuteBatchCAS(
	b *Batch,
	previous map[string]any,
) (_ bool, _ Iter, retError error) {
	defer func() { s.handleError(retError) }()

	applied, iter, err := s.Value.Load().(*gocql.Session).MapExecuteBatchCAS(b.gocqlBatch, previous)
	return applied, iter, err
}

func (s *session) AwaitSchemaAgreement(
	ctx context.Context,
) (retError error) {
	defer func() { s.handleError(retError) }()

	if err := s.Value.Load().(*gocql.Session).AwaitSchemaAgreement(ctx); err != nil {
		if isMissingPeersV2TableError(err) {
			return nil
		}
		return err
	}
	return nil
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
	case gocql.ErrNoConnections,
		gocql.ErrSessionClosed:
		s.refresh()
	default:
		// noop
	}
}
