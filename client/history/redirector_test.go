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
		connections *MockconnectionPool
		resolver    *membership.MockServiceResolver
	}
)

func TestBasicRedirectorSuite(t *testing.T) {
	s := new(basicRedirectorSuite)
	suite.Run(t, s)
}

func (s *basicRedirectorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.connections = NewMockconnectionPool(s.controller)
	s.resolver = membership.NewMockServiceResolver(s.controller)
}

func (s *basicRedirectorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *basicRedirectorSuite) TestShardCheck() {
	r := newBasicRedirector(s.connections, s.resolver)

	invalErr := &serviceerror.InvalidArgument{}
	err := r.execute(
		context.Background(),
		-1,
		func(_ context.Context, _ historyservice.HistoryServiceClient) error {
			panic("notreached")
		})
	s.ErrorAs(err, &invalErr)

	_, err = r.clientForShardID(-1)
	s.ErrorAs(err, &invalErr)
}

func opErrorTest(s *basicRedirectorSuite, clientOp clientOperation, verify func(err error)) {
	testAddr := rpcAddress("testaddr")
	shardID := int32(1)

	s.resolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(string(testAddr)), nil).
		Times(1)

	mockClient := historyservicemock.NewMockHistoryServiceClient(s.controller)
	clientConn := clientConnection{
		historyClient: mockClient,
	}
	s.connections.EXPECT().
		getOrCreateClientConn(testAddr).
		Return(clientConn).
		Times(1)

	r := newBasicRedirector(s.connections, s.resolver)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := r.execute(ctx, shardID, clientOp)
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

	mockClient1 := historyservicemock.NewMockHistoryServiceClient(s.controller)
	mockClient2 := historyservicemock.NewMockHistoryServiceClient(s.controller)
	clientConn1 := clientConnection{
		historyClient: mockClient1,
	}
	clientConn2 := clientConnection{
		historyClient: mockClient2,
	}
	s.connections.EXPECT().
		getOrCreateClientConn(testAddr1).
		Return(clientConn1).
		Times(2)

	r := newBasicRedirector(s.connections, s.resolver)
	attempt := 1
	doExecute := func() error {
		return r.execute(
			context.Background(),
			shardID,
			func(ctx context.Context, client historyservice.HistoryServiceClient) error {
				switch attempt {
				case 1:
					if client != mockClient1 {
						return errors.New("wrong client")
					}
					attempt++
					return serviceerrors.NewShardOwnershipLost("", "current")
				case 2:
					if client != mockClient1 {
						return errors.New("wrong client")
					}
					attempt++
					s.connections.EXPECT().
						getOrCreateClientConn(testAddr2).
						Return(clientConn2).
						Times(1)
					return serviceerrors.NewShardOwnershipLost(string(testAddr2), "current")
				case 3:
					if client != mockClient2 {
						return errors.New("wrong client")
					}
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
	clientConn := clientConnection{
		historyClient: mockClient,
	}
	s.connections.EXPECT().
		getOrCreateClientConn(testAddr).
		Return(clientConn)

	r := newBasicRedirector(s.connections, s.resolver)
	cli, err := r.clientForShardID(shardID)
	s.NoError(err)
	s.Equal(mockClient, cli)
}
