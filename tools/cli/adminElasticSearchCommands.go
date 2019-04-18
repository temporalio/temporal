// Copyright (c) 2017 Uber Technologies, Inc.
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
	"github.com/olekukonko/tablewriter"
	"github.com/olivere/elastic"
	"github.com/uber/cadence/.gen/go/indexer"
	es "github.com/uber/cadence/common/elasticsearch"
	"github.com/urfave/cli"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	esDocIDDelimiter = "~"
	esDocType        = "_doc"

	versionTypeExternal = "external"
)

const (
	headerSource      = "rpc-caller"
	headerDestination = "rpc-service"
)

// muttleyTransport wraps around default http.Transport to add muttley specific headers to all requests
type muttleyTransport struct {
	http.Transport

	source      string
	destination string
}

func (t *muttleyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Set(headerSource, t.source)
	r.Header.Set(headerDestination, t.destination)
	return t.Transport.RoundTrip(r)
}

func getESClient(c *cli.Context) *elastic.Client {
	url := getRequiredOption(c, FlagURL)
	var client *elastic.Client
	var err error
	retrier := elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))
	if c.IsSet(FlagMuttleyDestination) {
		httpClient := &http.Client{
			Transport: &muttleyTransport{
				source:      "cadence-cli",
				destination: c.String(FlagMuttleyDestination),
			},
		}
		client, err = elastic.NewClient(
			elastic.SetHttpClient(httpClient),
			elastic.SetURL(url),
			elastic.SetRetrier(retrier),
		)
	} else {
		client, err = elastic.NewClient(
			elastic.SetURL(url),
			elastic.SetRetrier(retrier),
		)
	}
	if err != nil {
		ErrorAndExit("Unable to create ElasticSearch client", err)
	}
	return client
}

// AdminCatIndices cat indices for ES cluster
func AdminCatIndices(c *cli.Context) {
	esClient := getESClient(c)

	ctx := context.Background()
	resp, err := esClient.CatIndices().Do(ctx)
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

// AdminIndex used to bulk insert message from kafka parse
func AdminIndex(c *cli.Context) {
	esClient := getESClient(c)
	indexName := getRequiredOption(c, FlagIndex)
	inputFileName := getRequiredOption(c, FlagInputFile)
	batchSize := c.Int(FlagBatchSize)

	messages, err := parseIndexerMessage(inputFileName)
	if err != nil {
		ErrorAndExit("Unable to parse indexer message", err)
	}

	bulkRequest := esClient.Bulk()
	bulkConductFn := func() {
		_, err := bulkRequest.Do(context.Background())
		if err != nil {
			ErrorAndExit("Bulk failed", err)
		}
		if bulkRequest.NumberOfActions() != 0 {
			ErrorAndExit(fmt.Sprintf("Bulk request not done, %d", bulkRequest.NumberOfActions()), err)
		}
	}
	for i, message := range messages {
		docID := message.GetWorkflowID() + esDocIDDelimiter + message.GetRunID()
		var req elastic.BulkableRequest
		switch message.GetMessageType() {
		case indexer.MessageTypeIndex:
			doc := generateESDoc(message)
			req = elastic.NewBulkIndexRequest().
				Index(indexName).
				Type(esDocType).
				Id(docID).
				VersionType(versionTypeExternal).
				Version(message.GetVersion()).
				Doc(doc)
		case indexer.MessageTypeDelete:
			req = elastic.NewBulkDeleteRequest().
				Index(indexName).
				Type(esDocType).
				Id(docID).
				VersionType(versionTypeExternal).
				Version(message.GetVersion())
		default:
			ErrorAndExit("Unknown message type", nil)
		}
		bulkRequest.Add(req)

		if i%batchSize == batchSize-1 {
			bulkConductFn()
		}
	}
	if bulkRequest.NumberOfActions() != 0 {
		bulkConductFn()
	}
}

func parseIndexerMessage(fileName string) (messages []*indexer.Message, err error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	idx := 0
	for scanner.Scan() {
		idx++
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			fmt.Printf("line %v is empty, skipped\n", idx)
			continue
		}

		msg := &indexer.Message{}
		err := json.Unmarshal([]byte(line), msg)
		if err != nil {
			fmt.Printf("line %v cannot be deserialized to indexer message: %v.\n", idx, line)
			return nil, err
		}
		messages = append(messages, msg)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func generateESDoc(msg *indexer.Message) map[string]interface{} {
	doc := make(map[string]interface{})
	doc[es.DomainID] = msg.GetDomainID()
	doc[es.WorkflowID] = msg.GetWorkflowID()
	doc[es.RunID] = msg.GetRunID()

	for k, v := range msg.Fields {
		switch v.GetType() {
		case indexer.FieldTypeString:
			doc[k] = v.GetStringData()
		case indexer.FieldTypeInt:
			doc[k] = v.GetIntData()
		case indexer.FieldTypeBool:
			doc[k] = v.GetBoolData()
		case indexer.FieldTypeBinary:
			doc[k] = v.GetBinaryData()
		default:
			ErrorAndExit("Unknown field type", nil)
		}
	}
	return doc
}
