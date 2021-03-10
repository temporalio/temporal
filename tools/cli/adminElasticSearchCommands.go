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
	"go.uber.org/atomic"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	espersistence "go.temporal.io/server/common/persistence/elasticsearch"
	"go.temporal.io/server/common/persistence/elasticsearch/client"
	"go.temporal.io/server/common/persistence/elasticsearch/esql"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/service/config"
	dc "go.temporal.io/server/common/service/dynamicconfig"
)

const (
	esDocIDDelimiter = "~"
)

const (
	headerSource      = "rpc-caller"
	headerDestination = "rpc-service"
)

var timeKeys = map[string]bool{
	searchattribute.StartTime:     true,
	searchattribute.CloseTime:     true,
	searchattribute.ExecutionTime: true,
}

func timeKeyFilter(key string) bool {
	return timeKeys[key]
}

func timeValProcess(timeStr string) (string, error) {
	// first check if already in int64 format
	if _, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		return timeStr, nil
	}
	// try to parse time
	parsedTime, err := time.Parse(defaultDateTimeFormat, timeStr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", parsedTime.UnixNano()), nil
}

func newESClient(c *cli.Context) client.CLIClient {
	url := getRequiredOption(c, FlagURL)
	var version string
	if c.IsSet(FlagVersion) {
		version = c.String(FlagVersion)
	}

	client, err := client.NewCLIClient(url, version)
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
// {"NamespaceID": "namespace-id", "Namespace": "namespace-name", "Execution": {"workflow_id": "workflow-id1", "run_id": "run-id" }, "WorkflowTypeName": "workflow-type", "StartTimestamp": 1234, "Status": 1, "ExecutionTimestamp": 5678, "TaskID": 2208, "ShardID": 1978, "Memo": {"fields": {"memo-1": {"metadata": {"encoding": "anNvbi9wbGFpbg==" }, "data": "MQ==" } } }, "TaskQueue": "task-queue", "CloseTimestamp": 9012, "HistoryLength": 2203, "SearchAttributes": {"indexed_fields": {"CustomKeywordField": {"metadata": {"encoding": "anNvbi9wbGFpbg==" }, "data": "ImtleXdvcmQiCg==" } } } }
func AdminIndex(c *cli.Context) {
	esClient := newESClient(c)
	indexName := getRequiredOption(c, FlagIndex)
	inputFileName := getRequiredOption(c, FlagInputFile)
	numOfBatches := c.Int(FlagBatchSize)

	messages, err := parseIndexerMessage(inputFileName)
	if err != nil {
		ErrorAndExit("Unable to parse RecordWorkflowExecutionClosedRequest message", err)
	}

	esProcessorConfig := &espersistence.ProcessorConfig{
		IndexerConcurrency:       dc.GetIntPropertyFn(100),
		ESProcessorNumOfWorkers:  dc.GetIntPropertyFn(1),
		ESProcessorBulkActions:   dc.GetIntPropertyFn(numOfBatches),
		ESProcessorBulkSize:      dc.GetIntPropertyFn(2 << 20),
		ESProcessorFlushInterval: dc.GetDurationPropertyFn(1 * time.Second),
		ValidSearchAttributes:    dc.GetMapPropertyFn(searchattribute.GetDefaultTypeMap()),
	}

	logger, err := log.NewDevelopment()
	if err != nil {
		ErrorAndExit("Unable to create logger", err)
	}

	esProcessor := espersistence.NewProcessor(esProcessorConfig, esClient, logger, metrics.NewNoopMetricsClient())
	esProcessor.Start()

	visibilityConfigForES := &config.VisibilityConfig{
		ESIndexMaxResultWindow: dc.GetIntPropertyFn(10000),
		ValidSearchAttributes:  dc.GetMapPropertyFn(searchattribute.GetDefaultTypeMap()),
		ESProcessorAckTimeout:  dc.GetDurationPropertyFn(1 * time.Minute),
	}
	visibilityManager := espersistence.NewVisibilityManager(indexName, esClient, visibilityConfigForES, esProcessor, metrics.NewNoopMetricsClient(), logger)

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
		req := &client.BulkableRequest{
			Index:       indexName,
			ID:          docID,
			Version:     math.MaxInt64,
			RequestType: client.BulkableRequestTypeDelete,
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

func parseIndexerMessage(fileName string) (messages []*persistence.RecordWorkflowExecutionClosedRequest, err error) {
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

		msg := &persistence.RecordWorkflowExecutionClosedRequest{}
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

// This function is used to trim unnecessary tag in returned json for table header
func trimBucketKey(k string) string {
	// group key is in form of "group_key", we only need "key" as the column name
	if strings.HasPrefix(k, "group_") {
		k = k[6:]
	}
	if strings.HasPrefix(k, "Attr_") {
		k = k[5:]
	}
	return fmt.Sprintf(`%v(*)`, k)
}

// parse the returned time to readable string if time is in int64 format
func toTimeStr(s interface{}) string {
	floatTime, err := strconv.ParseFloat(s.(string), 64)
	intTime := int64(floatTime)
	if err != nil {
		return s.(string)
	}
	t := time.Unix(0, intTime).UTC()
	return t.Format(time.RFC3339)
}

// GenerateReport generate report for an aggregation query to ES
func GenerateReport(c *cli.Context) {
	// use url command argument to create client
	index := getRequiredOption(c, FlagIndex)
	sql := getRequiredOption(c, FlagListQuery)
	var reportFormat, reportFilePath string
	if c.IsSet(FlagOutputFormat) {
		reportFormat = c.String(FlagOutputFormat)
	}
	if c.IsSet(FlagOutputFilename) {
		reportFilePath = c.String(FlagOutputFilename)
	} else {
		reportFilePath = "./report." + reportFormat
	}

	// convert sql to dsl
	e := esql.NewESql()
	e.SetTemporal(true)
	e.ProcessQueryValue(timeKeyFilter, timeValProcess)
	dsl, sortFields, err := e.ConvertPrettyTemporal(sql, "")
	if err != nil {
		ErrorAndExit("Fail to convert sql to dsl", err)
	}

	esClient := newESClient(c)
	ctx := context.Background()
	// query client
	resp, err := esClient.SearchWithDSL(ctx, index, dsl)
	if err != nil {
		ErrorAndExit("Fail to talk with ES", err)
	}

	// Show result to terminal
	table := tablewriter.NewWriter(os.Stdout)
	var headers []string
	var groupby, bucket map[string]interface{}
	var buckets []interface{}
	err = json.Unmarshal(resp.Aggregations["groupby"], &groupby)
	if err != nil {
		ErrorAndExit("Fail to parse groupby", err)
	}
	buckets = groupby["buckets"].([]interface{})
	if len(buckets) == 0 {
		fmt.Println("no matching bucket")
		return
	}

	// get the FIRST bucket in bucket list to extract all tags. These extracted tags are to be used as table heads
	bucket = buckets[0].(map[string]interface{})
	// record the column position in the table of each returned item
	ids := make(map[string]int)
	// We want these 3 columns shows at leftmost of the table in temporal report usage. It can be changed in future.
	primaryCols := []string{"group_NamespaceId", "group_WorkflowType", "group_Status"}
	primaryColsMap := map[string]int{
		"group_NamespaceId":  1,
		"group_WorkflowType": 1,
		"group_Status":       1,
	}
	buckKeys := 0 // number of bucket keys, used for table collapsing in html report
	if v, exist := bucket["key"]; exist {
		vmap := v.(map[string]interface{})
		// first search whether primaryCols keys exist, if found, put them at the table beginning
		for _, k := range primaryCols {
			if _, exist := vmap[k]; exist {
				k = trimBucketKey(k) // trim the unnecessary prefix
				headers = append(headers, k)
				ids[k] = len(ids)
				buckKeys++
			}
		}
		// extract all remaining bucket keys
		for k := range vmap {
			if _, exist := primaryColsMap[k]; !exist {
				k = trimBucketKey(k)
				headers = append(headers, k)
				ids[k] = len(ids)
				buckKeys++
			}
		}
	}
	// extract all other non-key items and set the table head accordingly
	for k := range bucket {
		if k != "key" {
			if k == "doc_count" {
				k = "count"
			}
			headers = append(headers, k)
			ids[k] = len(ids)
		}
	}
	table.SetHeader(headers)

	// read each bucket and fill the table, use map ids to find the correct spot
	var tableData [][]string
	for _, b := range buckets {
		bucket = b.(map[string]interface{})
		data := make([]string, len(headers))
		for k, v := range bucket {
			switch k {
			case "key": // fill group key
				vmap := v.(map[string]interface{})
				for kk, vv := range vmap {
					kk = trimBucketKey(kk)
					data[ids[kk]] = fmt.Sprintf("%v", vv)
				}
			case "doc_count": // fill bucket size count
				data[ids["count"]] = fmt.Sprintf("%v", v)
			default:
				var datum string
				vmap := v.(map[string]interface{})
				if strings.Contains(k, "Attr_CustomDatetimeField") {
					datum = fmt.Sprintf("%v", vmap["value_as_string"])
				} else {
					datum = fmt.Sprintf("%v", vmap["value"])
					// convert Temporal stored time (unix nano) to readable format
					if strings.Contains(k, "Time") && !strings.Contains(k, "Attr_") {
						datum = toTimeStr(datum)
					}
				}
				data[ids[k]] = datum
			}
		}
		table.Append(data)
		tableData = append(tableData, data)
	}
	table.Render()

	switch reportFormat {
	case "html", "HTML":
		sorted := len(sortFields) > 0 || strings.Contains(sql, "ORDER BY") || strings.Contains(sql, "order by")
		generateHTMLReport(reportFilePath, buckKeys, sorted, headers, tableData)
	case "csv", "CSV":
		generateCSVReport(reportFilePath, headers, tableData)
	default:
		ErrorAndExit(fmt.Sprintf(`Report format %v not supported.`, reportFormat), nil)
	}
}

func generateCSVReport(reportFileName string, headers []string, tableData [][]string) {
	// write csv report
	f, err := os.Create(reportFileName)
	if err != nil {
		ErrorAndExit("Fail to create csv report file", err)
	}
	csvContent := strings.Join(headers, ",") + "\n"
	for _, data := range tableData {
		csvContent += strings.Join(data, ",") + "\n"
	}
	_, err = f.WriteString(csvContent)
	if err != nil {
		fmt.Printf("Error write to file, err: %v", err)
	}
	f.Close()
}

func generateHTMLReport(reportFileName string, numBuckKeys int, sorted bool, headers []string, tableData [][]string) {
	// write html report
	f, err := os.Create(reportFileName)
	if err != nil {
		ErrorAndExit("Fail to create html report file", err)
	}
	var htmlContent string
	m, n := len(headers), len(tableData)
	rowSpan := make([]int, m) // record the collapsing size of each column
	for i := 0; i < m; i++ {
		rowSpan[i] = 1
		cell := wrapWithTag(headers[i], "td", "")
		htmlContent += cell
	}
	htmlContent = wrapWithTag(htmlContent, "tr", "")

	for row := 0; row < n; row++ {
		var rowData string
		for col := 0; col < m; col++ {
			rowSpan[col]--
			// don't do collapsing if sorted
			if col < numBuckKeys-1 && !sorted {
				if rowSpan[col] == 0 {
					for i := row; i < n; i++ {
						if tableData[i][col] == tableData[row][col] {
							rowSpan[col]++
						} else {
							break
						}
					}
					var property string
					if rowSpan[col] > 1 {
						property = fmt.Sprintf(`rowspan="%d"`, rowSpan[col])
					}
					cell := wrapWithTag(tableData[row][col], "td", property)
					rowData += cell
				}
			} else {
				cell := wrapWithTag(tableData[row][col], "td", "")
				rowData += cell
			}
		}
		rowData = wrapWithTag(rowData, "tr", "")
		htmlContent += rowData
	}
	htmlContent = wrapWithTag(htmlContent, "table", "")
	htmlContent = wrapWithTag(htmlContent, "body", "")
	htmlContent = wrapWithTag(htmlContent, "html", "")

	//nolint:errcheck
	f.WriteString("<!DOCTYPE html>\n")
	f.WriteString(`<head>
	<style>
	table, th, td {
	  border: 1px solid black;
	  padding: 5px;
	}
	table {
	  border-spacing: 2px;
	}
	</style>
	</head>` + "\n")
	f.WriteString(htmlContent)
	f.Close()

}

// return a string that use tag to wrap content
func wrapWithTag(content string, tag string, property string) string {
	if property != "" {
		property = " " + property
	}
	if tag != "td" {
		content = "\n" + content
	}
	return "<" + tag + property + ">" + content + "</" + tag + ">\n"
}
