package history

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/membership"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.uber.org/mock/gomock"
)

type (
	basicRedirectorSuite struct {
		suite.Suite
		*require.Assertions

		controller  *gomock.Controller
		connections *mockConnectionPool[historyservice.HistoryServiceClient]
		resolver    *membership.MockServiceResolver
	}
)

type mockConnectionPool[C any] struct {
	connectionPool[C]
	client     C
	resetCalls int
}

func (m *mockConnectionPool[C]) getOrCreateClientConn(testAddr rpcAddress) clientConnection[C] {
	return clientConnection[C]{grpcClient: m.client}
}

func (m *mockConnectionPool[C]) resetConnectBackoff(clientConnection[C]) {
	m.resetCalls++
}

func TestBasicRedirectorSuite(t *testing.T) {
	s := new(basicRedirectorSuite)
	suite.Run(t, s)
}

func (s *basicRedirectorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.resolver = membership.NewMockServiceResolver(s.controller)
	s.connections = &mockConnectionPool[historyservice.HistoryServiceClient]{}
}

func (s *basicRedirectorSuite) TestShardCheck() {
	r := NewBasicRedirector(s.connections, s.resolver)

	invalErr := &serviceerror.InvalidArgument{}
	err := r.Execute(
		context.Background(),
		-1,
		func(_ context.Context, _ historyservice.HistoryServiceClient) error {
			panic("notreached")
		})
	s.ErrorAs(err, &invalErr)

	_, err = r.clientForShardID(-1)
	s.ErrorAs(err, &invalErr)
}

func opErrorTest(s *basicRedirectorSuite, clientOp ClientOperation[historyservice.HistoryServiceClient], verify func(err error)) {
	testAddr := rpcAddress("testaddr")
	shardID := int32(1)

	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr)), nil).
		Times(1)

	mockClient := historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.connections.client = mockClient

	r := NewBasicRedirector(s.connections, s.resolver)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := r.Execute(ctx, shardID, clientOp)
	verify(err)
}

func (s *basicRedirectorSuite) TestDeadlineExceededError() {
	opErrorTest(s,
		func(ctx context.Context, client historyservice.HistoryServiceClient) error {
			<-ctx.Done()
			return ctx.Err()
		},
		func(err error) {
			s.ErrorIs(err, context.DeadlineExceeded)
		})
}

func (s *basicRedirectorSuite) TestUnavailableError() {
	opErrorTest(s,
		func(ctx context.Context, client historyservice.HistoryServiceClient) error {
			return serviceerror.NewUnavailable("unavail")
		},
		func(err error) {
			unavil := &serviceerror.Unavailable{}
			s.ErrorAs(err, &unavil)
		})
}

func (s *basicRedirectorSuite) TestShardOwnershipLostErrors() {
	testAddr1 := rpcAddress("testaddr1")
	testAddr2 := rpcAddress("testaddr2")
	shardID := int32(1)

	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr1)), nil).
		Times(2)

	mockClient := historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.connections.client = mockClient

	r := NewBasicRedirector(s.connections, s.resolver)
	attempt := 1
	doExecute := func() error {
		return r.Execute(
			context.Background(),
			shardID,
			func(ctx context.Context, client historyservice.HistoryServiceClient) error {
				switch attempt {
				case 1:
					attempt++
					return serviceerrors.NewShardOwnershipLost("", "current")
				case 2:
					attempt++
					return serviceerrors.NewShardOwnershipLost(string(testAddr2), "current")
				case 3:
					attempt++
					return nil
				}
				return errors.New("too many attempts")
			},
		)
	}
	err := doExecute()
	s.Error(err)
	solErr := &serviceerrors.ShardOwnershipLost{}
	s.ErrorAs(err, &solErr)

	err = doExecute()
	s.NoError(err)
	s.Equal(4, attempt)
}

func (s *basicRedirectorSuite) TestClientForTargetByShard() {
	testAddr := rpcAddress("testaddr")
	shardID := int32(1)

	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr)), nil).
		Times(1)

	mockClient := historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.connections.client = mockClient
	r := NewBasicRedirector(s.connections, s.resolver)
	cli, err := r.clientForShardID(shardID)
	s.NoError(err)
	s.Equal(mockClient, cli)
}
