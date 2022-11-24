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

package host

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/tests/testhelper"
)

func TestAcquireShardError(t *testing.T) {
	for _, c := range []struct {
		Name             string
		Err              error
		ExpectedAttempts int
	}{
		{
			Name:             "TimeoutError",
			Err:              context.DeadlineExceeded,
			ExpectedAttempts: 3,
		},
		{
			Name:             "ShardOwnershipLostError",
			Err:              &persistence.ShardOwnershipLostError{},
			ExpectedAttempts: 1,
		},
		{
			Name:             "NoError",
			Err:              nil,
			ExpectedAttempts: 1,
		},
	} {
		t.Run(c.Name, func(t *testing.T) {

			var env string
			switch TestFlags.PersistenceType {
			case config.StoreTypeSQL:
				switch TestFlags.PersistenceDriver {
				case mysql.PluginName:
					env = "development-mysql"
				case postgresql.PluginName:
					env = "development-postgres"
				case sqlite.PluginName:
					env = "development-sqlite"
				default:
					t.Fatalf("unknown sql store driver: %v", TestFlags.PersistenceDriver)
				}
			case config.StoreTypeNoSQL:
				env = "development-cass"
			default:
				t.Fatalf("unknown persistence store type: %v", TestFlags.PersistenceType)
			}
			configDir := filepath.Join(testhelper.GetRepoRootDirectory(), "config")
			cfg, err := config.LoadConfig(env, configDir, "")
			require.NoError(t, err)
			updates := make(chan struct{})
			server := temporal.NewServer(
				temporal.WithConfig(cfg),
				temporal.ForServices([]string{common.HistoryServiceName}),
				temporal.WithDynamicConfigClient(newTestDCClient(dynamicconfig.NewNoopClient())),
				temporal.WithLogger(log.NewNoopLogger()),
				temporal.WithFxOptions(
					fx.Decorate(func(fn client.FactoryProviderFn) client.FactoryProviderFn {
						return func(params client.NewFactoryParams) client.Factory {
							factory := fn(params)
							return &faultyPersistenceFactory{Factory: factory, Err: c.Err, Updates: updates}
						}
					}),
				),
			)
			err = server.Start()
			require.Nil(t, err)
			go func() {
				// TODO: remove this sleep once we have a way of overriding the retry policy
				// three tries occurring at 0s, 1s and 2s means that the third attempt should happen
				// after 3s, but we wait 4s to be safe
				time.Sleep(4 * time.Second)
				server.Stop()
				close(updates)
			}()
			nUpdates := 0
			for range updates {
				nUpdates++
			}
			assert.Equal(t, int(cfg.Persistence.NumHistoryShards)*c.ExpectedAttempts, nUpdates)
		})
	}
}

type faultyPersistenceFactory struct {
	client.Factory
	Err     error
	Updates chan struct{}
}

func (f *faultyPersistenceFactory) NewShardManager() (persistence.ShardManager, error) {
	shardManager, err := f.Factory.NewShardManager()
	if err != nil {
		return nil, err
	}
	return &faultyShardManager{ShardManager: shardManager, Err: f.Err, Updates: f.Updates}, nil
}

type faultyShardManager struct {
	UpdateShardCalls *int
	persistence.ShardManager
	Err     error
	Updates chan struct{}
}

func (f *faultyShardManager) UpdateShard(ctx context.Context, req *persistence.UpdateShardRequest) error {
	f.Updates <- struct{}{}
	if f.Err != nil {
		return f.Err
	}
	return f.ShardManager.UpdateShard(ctx, req)
}
