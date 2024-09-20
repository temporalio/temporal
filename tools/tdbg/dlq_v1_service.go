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

package tdbg

import (
	"fmt"
	"io"

	"github.com/urfave/cli/v2"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	repication "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/collection"
	"go.uber.org/multierr"
)

type DLQV1Service struct {
	clientFactory ClientFactory
	prompter      *Prompter
	writer        io.Writer
}

func NewDLQV1Service(clientFactory ClientFactory, prompter *Prompter, writer io.Writer) *DLQV1Service {
	return &DLQV1Service{
		clientFactory: clientFactory,
		prompter:      prompter,
		writer:        writer,
	}
}

func (ac *DLQV1Service) ReadMessages(c *cli.Context) (err error) {
	ctx, cancel := newContext(c)
	defer cancel()

	adminClient := ac.clientFactory.AdminClient(c)
	dlqType := c.String(FlagDLQType)
	sourceCluster := c.String(FlagCluster)
	shardID := c.Int(FlagShardID)
	outputFile, err := getOutputFile(c.String(FlagOutputFilename), ac.writer)
	if err != nil {
		return err
	}
	defer func() {
		// see https://pkg.go.dev/go.uber.org/multierr#hdr-Deferred_Functions
		err = multierr.Combine(err, outputFile.Close())
	}()

	remainingMessageCount := common.EndMessageID
	if c.IsSet(FlagMaxMessageCount) {
		remainingMessageCount = c.Int64(FlagMaxMessageCount)
	}
	var lastMessageID int64
	if c.IsSet(FlagLastMessageID) {
		lastMessageID = c.Int64(FlagLastMessageID)
	} else {
		ac.prompter.Prompt("Are you sure to read all DLQ messages without a upper boundary?")
		lastMessageID = common.EndMessageID
	}

	paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
		t, err := toQueueType(dlqType)
		if err != nil {
			return nil, nil, err
		}
		resp, err := adminClient.GetDLQMessages(ctx, &adminservice.GetDLQMessagesRequest{
			Type:                  t,
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
			return fmt.Errorf("unable to read dlq message. Last read message id: %v, Error: %v", lastReadMessageID, err)
		}

		task := item.(*repication.ReplicationTask)
		encoder := codec.NewJSONPBIndentEncoder(" ")
		taskStr, err := encoder.Encode(task)
		if err != nil {
			return fmt.Errorf("unable to encode dlq message. Last read message id: %v", lastReadMessageID)
		}

		lastReadMessageID = int(task.SourceTaskId)
		remainingMessageCount--
		_, err = outputFile.Write([]byte(fmt.Sprintf("%v\n", string(taskStr))))
		if err != nil {
			return fmt.Errorf("fail to print dlq messages.: %s", err)
		}
	}
	return nil
}

func (ac *DLQV1Service) PurgeMessages(c *cli.Context) error {
	ctx, cancel := newContext(c)
	defer cancel()

	dlqType := c.String(FlagDLQType)
	sourceCluster := c.String(FlagCluster)
	shardID := c.Int(FlagShardID)

	var lastMessageID int64
	if c.IsSet(FlagLastMessageID) {
		lastMessageID = c.Int64(FlagLastMessageID)
	} else {
		ac.prompter.Prompt("Are you sure to purge all DLQ messages without a upper boundary?")
	}

	adminClient := ac.clientFactory.AdminClient(c)
	t, err := toQueueType(dlqType)
	if err != nil {
		return err
	}
	if _, err := adminClient.PurgeDLQMessages(ctx, &adminservice.PurgeDLQMessagesRequest{
		Type:                  t,
		SourceCluster:         sourceCluster,
		ShardId:               int32(shardID),
		InclusiveEndMessageId: lastMessageID,
	}); err != nil {
		return fmt.Errorf("failed to purge DLQ")
	}
	fmt.Fprintln(c.App.Writer, "Successfully purged DLQ Messages.")
	return nil
}

func (ac *DLQV1Service) MergeMessages(c *cli.Context) error {
	ctx, cancel := newContext(c)
	defer cancel()

	dlqType := c.String(FlagDLQType)
	sourceCluster := c.String(FlagCluster)
	shardID := c.Int(FlagShardID)

	var lastMessageID int64
	if c.IsSet(FlagLastMessageID) {
		lastMessageID = c.Int64(FlagLastMessageID)
	} else {
		ac.prompter.Prompt("Are you sure to merge all DLQ messages without a upper boundary?")
	}

	adminClient := ac.clientFactory.AdminClient(c)

	t, err := toQueueType(dlqType)
	if err != nil {
		return err
	}

	request := &adminservice.MergeDLQMessagesRequest{
		Type:                  t,
		SourceCluster:         sourceCluster,
		ShardId:               int32(shardID),
		InclusiveEndMessageId: lastMessageID,
		MaximumPageSize:       defaultPageSize,
	}

	var response *adminservice.MergeDLQMessagesResponse
	for response == nil || len(response.GetNextPageToken()) > 0 {
		response, err = adminClient.MergeDLQMessages(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to merge DLQ message: %s", err)
		}

		request.NextPageToken = response.NextPageToken
		fmt.Fprintf(c.App.Writer, "Successfully merged %v messages. More messages to merge.\n", defaultPageSize)
	}
	fmt.Fprintln(c.App.Writer, "Successfully merged all messages.")
	return nil
}

func (ac *DLQV1Service) ListQueues(c *cli.Context) error {
	return serviceerror.NewUnimplemented("ListQueues is not implemented for DLQ v1")
}
