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
		*require.Assertions

		factory *sql.Factory
	}
)

func newConnectionSuite(
	t *testing.T,
	factory *sql.Factory,
) *connectionSuite {
	return &connectionSuite{
		Assertions: require.New(t),
		factory:    factory,
	}
}

func (s *connectionSuite) SetupSuite() {

}

func (s *connectionSuite) TearDownSuite() {

}

func (s *connectionSuite) SetupTest() {
	s.Assertions = require.New(s.T())
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
	s.NoError(err)

	store.Close() // Connection will be closed by this call
	manager := p.NewShardManager(store, serialization.NewSerializer())

	resp, err := manager.GetOrCreateShard(ctx, &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	})

	s.Nil(resp)
	s.ErrorContains(err, sqlplugin.DatabaseUnavailableError.Error())
}
