package tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type (
	connectionSuite struct {
		suite.Suite

		factory *sql.Factory
	}
)

func newConnectionSuite(
	t *testing.T,
	factory *sql.Factory,
) *connectionSuite {
	return &connectionSuite{

		factory: factory,
	}
}



func (s *connectionSuite) TearDownSuite() {

}



func (s *connectionSuite) TearDownTest() {

}

// Tests that SQL operations do not panic if the underlying connection has been closed and that the persistence layer
// returns a useful error message.
// Currently only run against MySQL and Postgresql (SQLite always maintains at least one open connection)
func (s *connectionSuite) TestClosedConnectionError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	shardID := (int32)(1)
	rangeID := rand.Int63()
	shardInfo := RandomShardInfo(shardID, rangeID)

	store, err := s.factory.NewShardStore()
	require.NoError(s.T(), err)

	store.Close() // Connection will be closed by this call
	manager := p.NewShardManager(store, serialization.NewSerializer())

	resp, err := manager.GetOrCreateShard(ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	})

	require.Nil(s.T(), resp)
	require.ErrorContains(s.T(), err, sqlplugin.DatabaseUnavailableError.Error())
}
