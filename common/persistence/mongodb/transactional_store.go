package mongodb

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

type transactionalStore struct {
	client         client.Client
	metricsHandler metrics.Handler
}

func newTransactionalStore(mongoClient client.Client, handlers ...metrics.Handler) transactionalStore {
	metricsHandler := metrics.NoopMetricsHandler
	if len(handlers) > 0 && handlers[0] != nil {
		metricsHandler = handlers[0]
	}
	return transactionalStore{client: mongoClient, metricsHandler: metricsHandler}
}

func (s *transactionalStore) executeTransaction(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	sess, err := s.client.StartSession(ctx)
	if err != nil {
		metrics.PersistenceMongoTransactionsFailed.With(s.metricsHandler).Record(1)
		metrics.PersistenceMongoSessionsInProgress.With(s.metricsHandler).Record(float64(s.client.NumberSessionsInProgress()))
		return nil, serviceerror.NewUnavailablef("failed to start mongo session: %v", err)
	}
	metrics.PersistenceMongoSessionsInProgress.With(s.metricsHandler).Record(float64(s.client.NumberSessionsInProgress()))
	defer func() {
		sess.EndSession(ctx)
		metrics.PersistenceMongoSessionsInProgress.With(s.metricsHandler).Record(float64(s.client.NumberSessionsInProgress()))
	}()

	metrics.PersistenceMongoTransactionsStarted.With(s.metricsHandler).Record(1)

	result, err := sess.WithTransaction(ctx, fn)
	if err != nil {
		metrics.PersistenceMongoTransactionsFailed.With(s.metricsHandler).Record(1)
		return nil, err
	}
	return result, nil
}
