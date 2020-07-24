// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cli

import (
	"encoding/json"
	"fmt"

	"github.com/urfave/cli"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/cassandra"
	"github.com/uber/cadence/common/quotas"
)

// AdminTimers is used to list scheduled timers.
func AdminTimers(c *cli.Context) {
	shardID := getRequiredIntOption(c, FlagShardID)

	startDate := c.String(FlagStartDate)
	endDate := c.String(FlagEndDate)
	rps := c.Int(FlagRPS)
	domainID := c.String(FlagDomainID)
	batchSize := c.Int(FlagBatchSize)
	timerTypes := c.IntSlice(FlagTimerType)
	skipErrMode := c.Bool(FlagSkipErrorMode)

	if !c.IsSet(FlagTimerType) || (len(timerTypes) == 1 && timerTypes[0] == -1) {
		timerTypes = []int{
			persistence.TaskTypeDecisionTimeout,
			persistence.TaskTypeActivityTimeout,
			persistence.TaskTypeUserTimer,
			persistence.TaskTypeWorkflowTimeout,
			persistence.TaskTypeDeleteHistoryEvent,
			persistence.TaskTypeActivityRetryTimer,
			persistence.TaskTypeWorkflowBackoffTimer,
		}
	}

	taskTypes := intSliceToSet(timerTypes)

	st, err := parseSingleTs(startDate)
	if err != nil {
		ErrorAndExit("wrong date format for "+FlagStartDate, err)
	}
	et, err := parseSingleTs(endDate)
	if err != nil {
		ErrorAndExit("wrong date format for "+FlagEndDate, err)
	}

	session := connectToCassandra(c)
	defer session.Close()

	limiter := quotas.NewSimpleRateLimiter(rps)
	logger := loggerimpl.NewNopLogger()

	execStore, err := cassandra.NewWorkflowExecutionPersistence(shardID, session, logger)
	if err != nil {
		ErrorAndExit("cannot get execution store", err)
	}

	ratelimitedClient := persistence.NewWorkflowExecutionPersistenceRateLimitedClient(
		persistence.NewExecutionManagerImpl(execStore, logger),
		limiter,
		logger,
	)

	var token []byte
	isFirstIteration := true
	for isFirstIteration || len(token) != 0 {
		isFirstIteration = false
		req := persistence.GetTimerIndexTasksRequest{
			MinTimestamp:  st,
			MaxTimestamp:  et,
			BatchSize:     batchSize,
			NextPageToken: token,
		}

		resp := &persistence.GetTimerIndexTasksResponse{}

		op := func() error {
			var err error
			resp, err = ratelimitedClient.GetTimerIndexTasks(&req)
			return err
		}

		err = backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)

		if err != nil {
			ErrorAndExit("cannot get timer tasks for shard", err)
		}

		token = resp.NextPageToken

		for _, t := range resp.Timers {
			if len(domainID) > 0 && t.DomainID != domainID {
				continue
			}

			if _, ok := taskTypes[t.TaskType]; !ok {
				continue
			}

			data, err := json.Marshal(t)
			if err != nil {
				if !skipErrMode {
					ErrorAndExit("cannot marshal timer to json", err)
				}
				fmt.Println(err.Error())
			} else {
				fmt.Println(string(data))
			}
		}
	}
}
