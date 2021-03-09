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
	"fmt"
	"os"

	"github.com/urfave/cli"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/collection"
)

const (
	defaultPageSize = 1000
)

// AdminGetDLQMessages gets DLQ metadata
func AdminGetDLQMessages(c *cli.Context) {
	ctx, cancel := newContext(c)
	defer cancel()

	adminClient := cFactory.AdminClient(c)
	dlqType := getRequiredOption(c, FlagDLQType)
	sourceCluster := getRequiredOption(c, FlagCluster)
	shardID := getRequiredIntOption(c, FlagShardID)
	outputFile := getOutputFile(c.String(FlagOutputFilename))
	defer outputFile.Close()

	remainingMessageCount := common.EndMessageID
	if c.IsSet(FlagMaxMessageCount) {
		remainingMessageCount = c.Int64(FlagMaxMessageCount)
	}
	var lastMessageID int64
	if c.IsSet(FlagLastMessageID) {
		lastMessageID = c.Int64(FlagLastMessageID)
	}

	paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
		resp, err := adminClient.GetDLQMessages(ctx, &adminservice.GetDLQMessagesRequest{
			Type:                  toQueueType(dlqType),
			SourceCluster:         sourceCluster,
			ShardId:               int32(shardID),
			InclusiveEndMessageId: lastMessageID,
			MaximumPageSize:       defaultPageSize,
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

		task := item.(*replicationspb.ReplicationTask)
		encoder := codec.NewJSONPBIndentEncoder(" ")
		taskStr, err := encoder.Encode(task)
		if err != nil {
			ErrorAndExit(fmt.Sprintf("fail to encode dlq message. Last read message id: %v", lastReadMessageID), err)
		}

		lastReadMessageID = int(task.SourceTaskId)
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
	sourceCluster := getRequiredOption(c, FlagCluster)
	shardID := getRequiredIntOption(c, FlagShardID)

	var lastMessageID int64
	if c.IsSet(FlagLastMessageID) {
		lastMessageID = c.Int64(FlagLastMessageID)
	} else {
		confirmOrExit("Are you sure to purge all DLQ messages without a upper boundary?")
	}

	adminClient := cFactory.AdminClient(c)
	if _, err := adminClient.PurgeDLQMessages(ctx, &adminservice.PurgeDLQMessagesRequest{
		Type:                  toQueueType(dlqType),
		SourceCluster:         sourceCluster,
		ShardId:               int32(shardID),
		InclusiveEndMessageId: lastMessageID,
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
	sourceCluster := getRequiredOption(c, FlagCluster)
	shardID := getRequiredIntOption(c, FlagShardID)

	var lastMessageID int64
	if c.IsSet(FlagLastMessageID) {
		lastMessageID = c.Int64(FlagLastMessageID)
	} else {
		confirmOrExit("Are you sure to merge all DLQ messages without a upper boundary?")
	}

	adminClient := cFactory.AdminClient(c)
	request := &adminservice.MergeDLQMessagesRequest{
		Type:                  toQueueType(dlqType),
		SourceCluster:         sourceCluster,
		ShardId:               int32(shardID),
		InclusiveEndMessageId: lastMessageID,
		MaximumPageSize:       defaultPageSize,
	}

	var response *adminservice.MergeDLQMessagesResponse
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

func toQueueType(dlqType string) enumsspb.DeadLetterQueueType {
	switch dlqType {
	case "namespace":
		return enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE
	case "history":
		return enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION
	default:
		ErrorAndExit("The queue type is not supported.", fmt.Errorf("the queue type is not supported. Type: %v", dlqType))
	}
	return enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE
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

func getOutputFile(outputFile string) *os.File {
	if len(outputFile) == 0 {
		return os.Stdout
	}
	f, err := os.Create(outputFile)
	if err != nil {
		ErrorAndExit("failed to create output file", err)
	}
	return f
}
