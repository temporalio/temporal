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
	"io"
	"os"

	"github.com/urfave/cli"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/cassandra"
	"github.com/uber/cadence/common/quotas"
)

// Loader loads timer task information
type Loader interface {
	Load() []*persistence.TimerTaskInfo
}

// Printer prints timer task information
type Printer interface {
	Print(timers []*persistence.TimerTaskInfo) error
}

// Reporter wraps Loader, Printer and a filter on time task type and domainID
type Reporter struct {
	domainID   string
	timerTypes []int
	loader     Loader
	printer    Printer
	timeFormat string
}

type cassandraLoader struct {
	ctx *cli.Context
}

type fileLoader struct {
	ctx *cli.Context
}

type histogramPrinter struct {
	ctx        *cli.Context
	timeFormat string
}

type jsonPrinter struct {
	ctx *cli.Context
}

// NewCassLoader creates a new Loader to load timer task information from cassandra
func NewCassLoader(c *cli.Context) Loader {
	return &cassandraLoader{
		ctx: c,
	}
}

// NewFileLoader creates a new Loader to load timer task information from file
func NewFileLoader(c *cli.Context) Loader {
	return &fileLoader{
		ctx: c,
	}
}

// NewReporter creates a new Reporter
func NewReporter(domain string, timerTypes []int, loader Loader, printer Printer) *Reporter {
	return &Reporter{
		timerTypes: timerTypes,
		domainID:   domain,
		loader:     loader,
		printer:    printer,
	}
}

// NewHistogramPrinter creates a new Printer to display timer task information in a histogram
func NewHistogramPrinter(c *cli.Context, timeFormat string) Printer {
	return &histogramPrinter{
		ctx:        c,
		timeFormat: timeFormat,
	}
}

// NewJSONPrinter creates a new Printer to display timer task information in a JSON format
func NewJSONPrinter(c *cli.Context) Printer {
	return &jsonPrinter{
		ctx: c,
	}
}

func (r *Reporter) filter(timers []*persistence.TimerTaskInfo) []*persistence.TimerTaskInfo {
	taskTypes := intSliceToSet(r.timerTypes)

	for i, t := range timers {
		if len(r.domainID) > 0 && t.DomainID != r.domainID {
			timers[i] = nil
			continue
		}
		if _, ok := taskTypes[t.TaskType]; !ok {
			timers[i] = nil
			continue

		}
	}

	return timers
}

// Report loads, filters and prints timer tasks
func (r *Reporter) Report() error {
	return r.printer.Print(r.filter(r.loader.Load()))
}

// AdminTimers is used to list scheduled timers.
func AdminTimers(c *cli.Context) {
	timerTypes := c.IntSlice(FlagTimerType)
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

	// setup loader
	var loader Loader
	if !c.IsSet(FlagInputFile) {
		loader = NewCassLoader(c)
	} else {
		loader = NewFileLoader(c)
	}

	// setup printer
	var printer Printer
	if !c.Bool(FlagPrintJSON) {
		var timerFormat string
		if c.IsSet(FlagDateFormat) {
			timerFormat = c.String(FlagDateFormat)
		} else {
			switch c.String(FlagBucketSize) {
			case "day":
				timerFormat = "2006-01-02"
			case "hour":
				timerFormat = "2006-01-02T15"
			case "minute":
				timerFormat = "2006-01-02T15:04"
			case "second":
				timerFormat = "2006-01-02T15:04:05"
			default:
				ErrorAndExit("unknown bucket size: "+c.String(FlagBucketSize), nil)
			}
		}
		printer = NewHistogramPrinter(c, timerFormat)
	} else {
		printer = NewJSONPrinter(c)
	}

	reporter := NewReporter(c.String(FlagDomainID), timerTypes, loader, printer)
	if err := reporter.Report(); err != nil {
		ErrorAndExit("Reporter failed", err)
	}
}

func (jp *jsonPrinter) Print(timers []*persistence.TimerTaskInfo) error {
	for _, t := range timers {
		if t == nil {
			continue
		}
		data, err := json.Marshal(t)
		if err != nil {
			if !jp.ctx.Bool(FlagSkipErrorMode) {
				ErrorAndExit("cannot marshal timer to json", err)
			}
			fmt.Println(err.Error())
		} else {
			fmt.Println(string(data))
		}
	}
	return nil
}

func (cl *cassandraLoader) Load() []*persistence.TimerTaskInfo {
	rps := cl.ctx.Int(FlagRPS)
	batchSize := cl.ctx.Int(FlagBatchSize)
	startDate := cl.ctx.String(FlagStartDate)
	endDate := cl.ctx.String(FlagEndDate)
	shardID := getRequiredIntOption(cl.ctx, FlagShardID)

	st, err := parseSingleTs(startDate)
	if err != nil {
		ErrorAndExit("wrong date format for "+FlagStartDate, err)
	}
	et, err := parseSingleTs(endDate)
	if err != nil {
		ErrorAndExit("wrong date format for "+FlagEndDate, err)
	}

	session := connectToCassandra(cl.ctx)
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

	var timers []*persistence.TimerTaskInfo

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
		timers = append(timers, resp.Timers...)
	}
	return timers
}

func (fl *fileLoader) Load() []*persistence.TimerTaskInfo {
	file, err := os.Open(fl.ctx.String(FlagInputFile))
	if err != nil {
		ErrorAndExit("cannot open file", err)
	}
	var data []*persistence.TimerTaskInfo
	dec := json.NewDecoder(file)

	for {
		var timer persistence.TimerTaskInfo
		if err := dec.Decode(&timer); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
		}
		data = append(data, &timer)
	}
	return data
}

func (hp *histogramPrinter) Print(timers []*persistence.TimerTaskInfo) error {
	h := NewHistogram()
	for _, t := range timers {
		if t == nil {
			continue
		}
		h.Add(t.VisibilityTimestamp.Format(hp.timeFormat))
	}

	return h.Print(hp.ctx.Int(FlagShardMultiplier))
}
