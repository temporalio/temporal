package persistence_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.uber.org/mock/gomock"
)

func TestHistoryManager_InvalidBranchToken_ReturnsInvalidArgument(t *testing.T) {
	// Invalid protobuf bytes that will fail to parse
	invalidBranchToken := []byte{0xFF, 0xFF, 0xFF, 0xFF}

	tests := []struct {
		name     string
		testFunc func(t *testing.T, em p.ExecutionManager, invalidToken []byte)
	}{
		{
			name: "ReadHistoryBranch",
			testFunc: func(t *testing.T, em p.ExecutionManager, invalidToken []byte) {
				_, err := em.ReadHistoryBranch(context.Background(), &p.ReadHistoryBranchRequest{
					ShardID:     1,
					BranchToken: invalidToken,
					MinEventID:  1,
					MaxEventID:  100,
					PageSize:    100,
				})
				requireInvalidArgumentError(t, err, "ReadHistoryBranch")
			},
		},
		{
			name: "ReadHistoryBranchByBatch",
			testFunc: func(t *testing.T, em p.ExecutionManager, invalidToken []byte) {
				_, err := em.ReadHistoryBranchByBatch(context.Background(), &p.ReadHistoryBranchRequest{
					ShardID:     1,
					BranchToken: invalidToken,
					MinEventID:  1,
					MaxEventID:  100,
					PageSize:    100,
				})
				requireInvalidArgumentError(t, err, "ReadHistoryBranchByBatch")
			},
		},
		{
			name: "ReadHistoryBranchReverse",
			testFunc: func(t *testing.T, em p.ExecutionManager, invalidToken []byte) {
				_, err := em.ReadHistoryBranchReverse(context.Background(), &p.ReadHistoryBranchReverseRequest{
					ShardID:     1,
					BranchToken: invalidToken,
					MaxEventID:  100,
					PageSize:    100,
				})
				requireInvalidArgumentError(t, err, "ReadHistoryBranchReverse")
			},
		},
		{
			name: "ReadRawHistoryBranch",
			testFunc: func(t *testing.T, em p.ExecutionManager, invalidToken []byte) {
				_, err := em.ReadRawHistoryBranch(context.Background(), &p.ReadHistoryBranchRequest{
					ShardID:     1,
					BranchToken: invalidToken,
					MinEventID:  1,
					MaxEventID:  100,
					PageSize:    100,
				})
				requireInvalidArgumentError(t, err, "ReadRawHistoryBranch")
			},
		},
		{
			name: "ForkHistoryBranch",
			testFunc: func(t *testing.T, em p.ExecutionManager, invalidToken []byte) {
				_, err := em.ForkHistoryBranch(context.Background(), &p.ForkHistoryBranchRequest{
					ForkBranchToken: invalidToken,
					ForkNodeID:      2,
					Info:            "test",
					ShardID:         1,
					NamespaceID:     "test-namespace",
					NewRunID:        "test-run-id",
				})
				requireInvalidArgumentError(t, err, "ForkHistoryBranch")
			},
		},
		{
			name: "DeleteHistoryBranch",
			testFunc: func(t *testing.T, em p.ExecutionManager, invalidToken []byte) {
				err := em.DeleteHistoryBranch(context.Background(), &p.DeleteHistoryBranchRequest{
					ShardID:     1,
					BranchToken: invalidToken,
				})
				requireInvalidArgumentError(t, err, "DeleteHistoryBranch")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := mock.NewMockExecutionStore(ctrl)
			// Return a real HistoryBranchUtil that will parse the branch token
			store.EXPECT().GetHistoryBranchUtil().AnyTimes().Return(p.NewHistoryBranchUtil(serialization.NewSerializer()))

			em := p.NewExecutionManager(
				store,
				serialization.NewSerializer(),
				nil,
				log.NewNoopLogger(),
				dynamicconfig.GetIntPropertyFn(1024*1024),
				dynamicconfig.GetBoolPropertyFn(false),
			)

			tc.testFunc(t, em, invalidBranchToken)
		})
	}
}

func requireInvalidArgumentError(t *testing.T, err error, operation string) {
	t.Helper()
	require.Error(t, err, "%s should return an error for invalid branch token", operation)

	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgErr,
		"%s should return InvalidArgument error for invalid branch token, got: %T - %v",
		operation, err, err)

	require.Contains(t, err.Error(), "unable to parse branch token",
		"%s error message should contain 'unable to parse branch token'", operation)
}

func TestAppendHistoryNodes_TransactionIDMustExceedPrevTransactionID(t *testing.T) {
	serializer := serialization.NewSerializer()
	branchUtil := p.NewHistoryBranchUtil(serializer)
	branchToken, err := branchUtil.NewHistoryBranch("", "", "", uuid.NewString(), nil, nil, 0, 0, 0)
	require.NoError(t, err)
	events := []*historypb.HistoryEvent{{EventId: 5, Version: 1}}
	blob, err := serializer.SerializeEvents(events)
	require.NoError(t, err)

	tests := []struct {
		name              string
		prevTransactionID int64
		transactionID     int64
		valid             bool
	}{
		{name: "greater", prevTransactionID: 100, transactionID: 101, valid: true},
		{name: "no previous", prevTransactionID: 0, transactionID: 1, valid: true},
		{name: "equal", prevTransactionID: 100, transactionID: 100, valid: false},
		{name: "lower", prevTransactionID: 101, transactionID: 100, valid: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := mock.NewMockExecutionStore(ctrl)
			store.EXPECT().GetHistoryBranchUtil().AnyTimes().Return(branchUtil)
			if tc.valid {
				store.EXPECT().AppendHistoryNodes(gomock.Any(), gomock.Any()).Times(2).Return(nil)
			}
			em := p.NewExecutionManager(
				store,
				serializer,
				nil,
				log.NewNoopLogger(),
				dynamicconfig.GetIntPropertyFn(1024*1024),
				dynamicconfig.GetBoolPropertyFn(false),
			)

			_, err := em.AppendHistoryNodes(context.Background(), &p.AppendHistoryNodesRequest{
				BranchToken:       branchToken,
				Events:            events,
				PrevTransactionID: tc.prevTransactionID,
				TransactionID:     tc.transactionID,
			})
			_, rawErr := em.AppendRawHistoryNodes(context.Background(), &p.AppendRawHistoryNodesRequest{
				BranchToken:       branchToken,
				History:           blob,
				NodeID:            5,
				PrevTransactionID: tc.prevTransactionID,
				TransactionID:     tc.transactionID,
			})
			if tc.valid {
				require.NoError(t, err)
				require.NoError(t, rawErr)
				return
			}
			require.ErrorAs(t, err, new(*p.InvalidPersistenceRequestError))
			require.ErrorAs(t, rawErr, new(*p.InvalidPersistenceRequestError))
		})
	}
}
