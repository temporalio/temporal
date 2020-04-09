package history

import "github.com/stretchr/testify/mock"

// MockHistoryEngineFactory is mock implementation for HistoryEngineFactory
type MockHistoryEngineFactory struct {
	mock.Mock
}

// CreateEngine is mock implementation for CreateEngine of HistoryEngineFactory
func (_m *MockHistoryEngineFactory) CreateEngine(context ShardContext) Engine {
	ret := _m.Called(context)

	var r0 Engine
	if rf, ok := ret.Get(0).(func(ShardContext) Engine); ok {
		r0 = rf(context)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(Engine)
		}
	}

	return r0
}

var _ EngineFactory = (*MockHistoryEngineFactory)(nil)
