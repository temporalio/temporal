package xdc

import (
	"context"
	"sync"

	"go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/temporal"
)

type bufferedEventPersistenceInjector struct {
	mu     sync.RWMutex
	inject func(*persistence.UpdateWorkflowExecutionRequest)
}

func (i *bufferedEventPersistenceInjector) set(
	inject func(*persistence.UpdateWorkflowExecutionRequest),
) func() {
	i.mu.Lock()
	previous := i.inject
	i.inject = inject
	i.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			i.mu.Lock()
			i.inject = previous
			i.mu.Unlock()
		})
	}
}

func (i *bufferedEventPersistenceInjector) apply(request *persistence.UpdateWorkflowExecutionRequest) {
	i.mu.RLock()
	inject := i.inject
	i.mu.RUnlock()
	if inject != nil {
		inject(request)
	}
}

type bufferedEventPersistenceFactory struct {
	persistenceclient.Factory
	injector *bufferedEventPersistenceInjector
}

func (f *bufferedEventPersistenceFactory) NewExecutionManager() (persistence.ExecutionManager, error) {
	manager, err := f.Factory.NewExecutionManager()
	if err != nil {
		return nil, err
	}
	return &bufferedEventExecutionManager{
		ExecutionManager: manager,
		injector:         f.injector,
	}, nil
}

type bufferedEventExecutionManager struct {
	persistence.ExecutionManager
	injector *bufferedEventPersistenceInjector
}

func (m *bufferedEventExecutionManager) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	m.injector.apply(request)
	return m.ExecutionManager.UpdateWorkflowExecution(ctx, request)
}

func bufferedEventPersistenceServerOption(injector *bufferedEventPersistenceInjector) temporal.ServerOption {
	baseProvider := temporal.PersistenceFactoryProvider()
	return temporal.WithPersistenceFactoryProvider(func(params persistenceclient.NewFactoryParams) persistenceclient.Factory {
		factory := baseProvider(params)
		if params.ServiceName != primitives.HistoryService {
			return factory
		}
		return &bufferedEventPersistenceFactory{
			Factory:  factory,
			injector: injector,
		}
	})
}
