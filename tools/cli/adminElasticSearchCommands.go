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

package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
	"go.temporal.io/server/common/config"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/elasticsearch"
	esclient "go.temporal.io/server/common/persistence/visibility/elasticsearch/client"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/atomic"
)

const (
	esDocIDDelimiter = "~"
)

func newESClient(c *cli.Context) esclient.CLIClient {
	url := getRequiredOption(c, FlagURL)
	var version string
	if c.IsSet(FlagVersion) {
		version = c.String(FlagVersion)
	}

	client, err := esclient.NewCLIClient(url, version)
	if err != nil {
		ErrorAndExit("Unable to create Elasticsearch client", err)
	}

	return client
}

// AdminCatIndices cat indices for ES cluster
func AdminCatIndices(c *cli.Context) {
	esClient := newESClient(c)

	ctx := context.Background()
	resp, err := esClient.CatIndices(ctx)
	if err != nil {
		ErrorAndExit("Unable to cat indices", err)
	}

	table := tablewriter.NewWriter(os.Stdout)
	header := []string{"health", "status", "index", "pri", "rep", "docs.count", "docs.deleted", "store.size", "pri.store.size"}
	table.SetHeader(header)
	for _, row := range resp {
		data := make([]string, len(header))
		data[0] = row.Health
		data[1] = row.Status
		data[2] = row.Index
		data[3] = strconv.Itoa(row.Pri)
		data[4] = strconv.Itoa(row.Rep)
		data[5] = strconv.Itoa(row.DocsCount)
		data[6] = strconv.Itoa(row.DocsDeleted)
		data[7] = row.StoreSize
		data[8] = row.PriStoreSize
		table.Append(data)
	}
	table.Render()
}

// AdminIndex used to bulk insert message to ES.
// Sample JSON line:
// {"NamespaceID": "namespace-id", "Namespace": "namespace-name", "Execution": {"workflow_id": "workflow-id1", "run_id": "run-id" }, "WorkflowTypeName": "workflow-type", "StartTime": 1234, "Status": 1, "ExecutionTime": 5678, "TaskID": 2208, "ShardID": 1978, "Memo": {"fields": {"memo-1": {"metadata": {"encoding": "anNvbi9wbGFpbg==" }, "data": "MQ==" } } }, "TaskQueue": "task-queue", "CloseTime": 9012, "HistoryLength": 2203, "SearchAttributes": {"indexed_fields": {"CustomKeywordField": {"metadata": {"encoding": "anNvbi9wbGFpbg==" }, "data": "ImtleXdvcmQiCg==" } } } }
func AdminIndex(c *cli.Context) {
	esClient := newESClient(c)
	indexName := getRequiredOption(c, FlagIndex)
	inputFileName := getRequiredOption(c, FlagInputFile)
	numOfBatches := c.Int(FlagBatchSize)

	messages, err := parseIndexerMessage(inputFileName)
	if err != nil {
		ErrorAndExit("Unable to parse RecordWorkflowExecutionClosedRequest message", err)
	}

	esProcessorConfig := &elasticsearch.ProcessorConfig{
		IndexerConcurrency:       dc.GetIntPropertyFn(100),
		ESProcessorNumOfWorkers:  dc.GetIntPropertyFn(1),
		ESProcessorBulkActions:   dc.GetIntPropertyFn(numOfBatches),
		ESProcessorBulkSize:      dc.GetIntPropertyFn(2 << 20),
		ESProcessorFlushInterval: dc.GetDurationPropertyFn(1 * time.Second),
	}

	logger := log.NewCLILogger()

	esProcessor := elasticsearch.NewProcessor(esProcessorConfig, esClient, logger, metrics.NewNoopMetricsClient())
	esProcessor.Start()

	visibilityConfigForES := &config.VisibilityConfig{
		ESProcessorAckTimeout: dc.GetDurationPropertyFn(1 * time.Minute),
	}

	// TODO: build search attribute provider to get search attributes from command line args.
	visibilityManager := elasticsearch.NewVisibilityManager(indexName, esClient, visibilityConfigForES, searchattribute.NewSystemProvider(), esProcessor, metrics.NewNoopMetricsClient(), logger)

	successLines := &atomic.Int32{}
	wg := &sync.WaitGroup{}
	wg.Add(numOfBatches)
	for i := 0; i < numOfBatches; i++ {
		go func(batchNum int) {
			for line := batchNum; line < len(messages); line += numOfBatches {
				err1 := visibilityManager.RecordWorkflowExecutionClosed(messages[line])
				if err1 != nil {
					fmt.Printf("Unable to save row at line %d: %v", line, err1)
				} else {
					successLines.Inc()
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	visibilityManager.Close()
	fmt.Println("ES processor stopped.", successLines, "lines were saved successfully")
}

// AdminDelete used to delete documents from Elasticsearch with input of list result
func AdminDelete(c *cli.Context) {
	esClient := newESClient(c)
	indexName := getRequiredOption(c, FlagIndex)
	inputFileName := getRequiredOption(c, FlagInputFile)
	batchSize := c.Int(FlagBatchSize)
	rps := c.Int(FlagRPS)
	ratelimiter := quotas.NewDefaultOutgoingDynamicRateLimiter(
		func() float64 { return float64(rps) },
	)

	// This is only executed from the CLI by an admin user
	// #nosec
	file, err := os.Open(inputFileName)
	if err != nil {
		ErrorAndExit("Cannot open input file", nil)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	scanner.Scan() // skip first line
	i := 0

	bulkRequest := esClient.Bulk()
	bulkConductFn := func() {
		_ = ratelimiter.Wait(context.Background())
		err := bulkRequest.Do(context.Background())
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Bulk failed, current processed row %d", i), err)
		}
		if bulkRequest.NumberOfActions() != 0 {
			ErrorAndExit(fmt.Sprintf("Bulk request not done, current processed row %d", i), nil)
		}
	}

	for scanner.Scan() {
		line := strings.Split(scanner.Text(), "|")
		docID := strings.TrimSpace(line[1]) + esDocIDDelimiter + strings.TrimSpace(line[2])
		req := &esclient.BulkableRequest{
			Index:       indexName,
			ID:          docID,
			Version:     math.MaxInt64,
			RequestType: esclient.BulkableRequestTypeDelete,
		}
		bulkRequest.Add(req)
		if i%batchSize == batchSize-1 {
			bulkConductFn()
		}
		i++
	}
	if bulkRequest.NumberOfActions() != 0 {
		bulkConductFn()
	}
}

func parseIndexerMessage(fileName string) (messages []*visibility.RecordWorkflowExecutionClosedRequest, err error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	idx := 0
	for scanner.Scan() {
		idx++
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			fmt.Printf("line %v is empty, skipped\n", idx)
			continue
		}

		msg := &visibility.RecordWorkflowExecutionClosedRequest{}
		err := json.Unmarshal([]byte(line), msg)
		if err != nil {
			fmt.Printf("line %v cannot be deserialized to RecordWorkflowExecutionClosedRequest: %v.\n", idx, line)
			return nil, err
		}
		messages = append(messages, msg)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}
