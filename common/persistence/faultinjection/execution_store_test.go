package faultinjection

import (
	"testing"

	"github.com/stretchr/testify/require"
	mockp "go.temporal.io/server/common/persistence/mock"
	"go.uber.org/mock/gomock"
)

type runAlreadyExistsCheckingStore struct {
	*mockp.MockExecutionStore
}

func (runAlreadyExistsCheckingStore) SupportsCheckRunAlreadyExists() bool {
	return true
}

func TestFaultInjectionExecutionStore_SupportsCheckRunAlreadyExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	require.True(t, newFaultInjectionExecutionStore(runAlreadyExistsCheckingStore{mockp.NewMockExecutionStore(ctrl)}, nil).SupportsCheckRunAlreadyExists())
	require.False(t, newFaultInjectionExecutionStore(mockp.NewMockExecutionStore(ctrl), nil).SupportsCheckRunAlreadyExists())
}
