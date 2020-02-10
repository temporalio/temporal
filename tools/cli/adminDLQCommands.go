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
	"encoding/json"
	"fmt"
	"os"

	"github.com/urfave/cli"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/collection"
)

const (
	defaultPageSize = 1000
)

// AdminGetDLQMessages gets DLQ metadata
func AdminGetDLQMessages(c *cli.Context) {
	ctx, cancel := newContext(c)
	defer cancel()

	adminClient := cFactory.ServerAdminClient(c)
	dlqType := getRequiredOption(c, FlagDLQType)
	outputFile := getOutputFile(c.String(FlagOutputFilename))
	defer outputFile.Close()

	remainingMessageCount := common.EndMessageID
	if c.IsSet(FlagMaxMessageCount) {
		remainingMessageCount = c.Int64(FlagMaxMessageCount)
	}
	var lastMessageID *int64
	if c.IsSet(FlagLastMessageID) {
		lastMessageID = common.Int64Ptr(c.Int64(FlagLastMessageID))
	}

	paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
		resp, err := adminClient.ReadDLQMessages(ctx, &replicator.ReadDLQMessagesRequest{
			Type:                  toQueueType(dlqType),
			InclusiveEndMessageID: lastMessageID,
			MaximumPageSize:       common.Int32Ptr(defaultPageSize),
			NextPageToken:         paginationToken,
		})
		if err != nil {
			return nil, nil, err
		}
		var paginateItems []interface{}
		for _, item := range resp.GetReplicationTasks() {
			paginateItems = append(paginateItems, item)
		}
		return paginateItems, resp.GetNextPageToken(), err
	}

	iterator := collection.NewPagingIterator(paginationFunc)
	var lastReadMessageID int
	for iterator.HasNext() && remainingMessageCount > 0 {
		item, err := iterator.Next()
		if err != nil {
			ErrorAndExit(fmt.Sprintf("fail to read dlq message. Last read message id: %v", lastReadMessageID), err)
		}

		task := item.(*replicator.ReplicationTask)
		taskStr, err := json.MarshalIndent(task, "", " ")
		if err != nil {
			ErrorAndExit(fmt.Sprintf("fail to encode dlq message. Last read message id: %v", lastReadMessageID), err)
		}

		lastReadMessageID = int(*task.SourceTaskId)
		remainingMessageCount--
		_, err = outputFile.WriteString(fmt.Sprintf("%v\n", string(taskStr)))
		if err != nil {
			ErrorAndExit("fail to print dlq messages.", err)
		}
	}
}

// AdminPurgeDLQMessages deletes messages from DLQ
func AdminPurgeDLQMessages(c *cli.Context) {
	ctx, cancel := newContext(c)
	defer cancel()

	dlqType := getRequiredOption(c, FlagDLQType)

	var lastMessageID *int64
	if c.IsSet(FlagLastMessageID) {
		lastMessageID = common.Int64Ptr(c.Int64(FlagLastMessageID))
	} else {
		confirmOrExit("Are you sure to purge all DLQ messages without a upper boundary?")
	}

	adminClient := cFactory.ServerAdminClient(c)
	if err := adminClient.PurgeDLQMessages(ctx, &replicator.PurgeDLQMessagesRequest{
		Type:                  toQueueType(dlqType),
		InclusiveEndMessageID: lastMessageID,
	}); err != nil {
		ErrorAndExit("Failed to purge dlq", nil)
	}
	fmt.Println("Successfully purge DLQ Messages.")
}

// AdminMergeDLQMessages merges message from DLQ
func AdminMergeDLQMessages(c *cli.Context) {
	ctx, cancel := newContext(c)
	defer cancel()

	dlqType := getRequiredOption(c, FlagDLQType)

	var lastMessageID *int64
	if c.IsSet(FlagLastMessageID) {
		lastMessageID = common.Int64Ptr(c.Int64(FlagLastMessageID))
	} else {
		confirmOrExit("Are you sure to merge all DLQ messages without a upper boundary?")
	}

	adminClient := cFactory.ServerAdminClient(c)
	request := &replicator.MergeDLQMessagesRequest{
		Type:                  toQueueType(dlqType),
		InclusiveEndMessageID: lastMessageID,
		MaximumPageSize:       common.Int32Ptr(defaultPageSize),
	}

	var response *replicator.MergeDLQMessagesResponse
	var err error
	for response == nil || len(response.GetNextPageToken()) > 0 {
		response, err = adminClient.MergeDLQMessages(ctx, request)
		if err != nil {
			ErrorAndExit("Failed to merge DLQ message", err)
		}

		request.NextPageToken = response.NextPageToken
		fmt.Printf("Successfully merged %v messages. More messages to merge.\n", defaultPageSize)
	}
	fmt.Println("Successfully merged all messages.")
}

func toQueueType(dlqType string) *replicator.DLQType {
	switch dlqType {
	case "domain":
		return replicator.DLQTypeDomain.Ptr()
	case "history":
		return replicator.DLQTypeReplication.Ptr()
	default:
		ErrorAndExit("The queue type is not supported.", fmt.Errorf("the queue type is not supported. Type: %v", dlqType))
	}
	return nil
}

func confirmOrExit(message string) {
	fmt.Println(message + " (Y/n)")
	reader := bufio.NewReader(os.Stdin)
	confirm, err := reader.ReadByte()
	if err != nil {
		panic(err)
	}
	if confirm != 'Y' {
		osExit(0)
	}
}
