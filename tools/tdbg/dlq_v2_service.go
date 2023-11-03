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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"

	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// DLQV2Service implements DLQService for [persistence.QueueV2].
	DLQV2Service struct {
		category        tasks.Category
		sourceCluster   string
		targetCluster   string
		clientFactory   ClientFactory
		writer          io.Writer
		prompter        *Prompter
		taskBlobEncoder TaskBlobEncoder
	}
	// DLQMessage is used primarily to form the JSON output of the `read` command. It's only used for v2.
	DLQMessage struct {
		// MessageID is the ID of the message within the DLQ. You can use this ID as an input to the `--last_message_id`
		// flag for the `purge` and `merge` commands.
		MessageID int64 `json:"message_id"`
		// ShardID is only used for non-namespace replication tasks.
		ShardID int32 `json:"shard_id"`
		// Payload contains the parsed task metadata from the server.
		Payload *TaskPayload `json:"payload"`
	}
	// TaskPayload implements both [json.Marshaler] and [json.Unmarshaler]. This allows us to pretty-print tasks using
	// jsonpb when serializing and then store the raw bytes of the task payload for later use when deserializing. We
	// need to store the raw bytes instead of immediately decoding to a concrete type because that logic is dynamic and
	// can't depend solely on the task category ID in case there are additional task categories in use.
	TaskPayload struct {
		taskBlobEncoder TaskBlobEncoder
		taskCategoryID  int
		blob            commonpb.DataBlob
		bytes           []byte
	}
)

var (
	_ json.Marshaler   = (*TaskPayload)(nil)
	_ json.Unmarshaler = (*TaskPayload)(nil)
)

func (p *TaskPayload) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	err := p.taskBlobEncoder.Encode(&b, p.taskCategoryID, p.blob)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (p *TaskPayload) UnmarshalJSON(b []byte) error {
	p.bytes = b
	return nil
}

// Bytes returns the raw bytes of the deserialized TaskPayload. This will return nil if the payload has not been
// deserialized yet.
func (p *TaskPayload) Bytes() []byte {
	return p.bytes
}

const dlqV2DefaultMaxMessageCount = 100

func NewDLQV2Service(
	category tasks.Category,
	sourceCluster string,
	targetCluster string,
	clientFactory ClientFactory,
	writer io.Writer,
	prompter *Prompter,
	taskBlobEncoder TaskBlobEncoder,
) *DLQV2Service {
	return &DLQV2Service{
		category:        category,
		sourceCluster:   sourceCluster,
		targetCluster:   targetCluster,
		clientFactory:   clientFactory,
		writer:          writer,
		prompter:        prompter,
		taskBlobEncoder: taskBlobEncoder,
	}
}

func newEncoder(writer io.Writer) *json.Encoder {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder
}

func (ac *DLQV2Service) ReadMessages(c *cli.Context) (err error) {
	ctx, cancel := newContext(c)
	defer cancel()

	remainingMessageCount := dlqV2DefaultMaxMessageCount
	if c.IsSet(FlagMaxMessageCount) {
		remainingMessageCount = c.Int(FlagMaxMessageCount)
		if remainingMessageCount <= 0 {
			return fmt.Errorf("--%s must be positive but was %d", FlagMaxMessageCount, remainingMessageCount)
		}
	}
	maxMessageID, err := ac.getLastMessageID(c, "read")
	if err != nil {
		return err
	}

	outputFile, err := getOutputFile(c.String(FlagOutputFilename), ac.writer)
	if err != nil {
		return err
	}
	defer func() {
		err = multierr.Append(err, outputFile.Close())
	}()
	adminClient := ac.clientFactory.AdminClient(c)

	pageSize := c.Int(FlagPageSize)
	iterator := collection.NewPagingIterator[*commonspb.HistoryDLQTask](
		func(paginationToken []byte) ([]*commonspb.HistoryDLQTask, []byte, error) {
			request := &adminservice.GetDLQTasksRequest{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  int32(ac.category.ID()),
					SourceCluster: ac.sourceCluster,
					TargetCluster: ac.targetCluster,
				},
				PageSize:      int32(pageSize),
				NextPageToken: paginationToken,
			}
			res, err := adminClient.GetDLQTasks(ctx, request)
			if err != nil {
				return nil, nil, fmt.Errorf("call to GetDLQTasks from ReadMessages failed: %w", err)
			}
			return res.DlqTasks, res.NextPageToken, nil
		},
	)

	for iterator.HasNext() && remainingMessageCount > 0 {
		dlqTask, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("DLQ task iterator returned error: %w", err)
		}
		if dlqTask.Metadata.MessageId > maxMessageID {
			break
		}
		blob := dlqTask.Payload.Blob
		if blob == nil {
			return fmt.Errorf("DLQ task payload blob is nil: %+v", dlqTask)
		}
		payload := &TaskPayload{
			taskBlobEncoder: ac.taskBlobEncoder,
			blob:            *blob,
			taskCategoryID:  ac.category.ID(),
		}
		message := DLQMessage{
			MessageID: dlqTask.Metadata.MessageId,
			ShardID:   dlqTask.Payload.ShardId,
			Payload:   payload,
		}
		err = newEncoder(outputFile).Encode(message)
		if err != nil {
			return err
		}
		remainingMessageCount--
		_, err = outputFile.Write([]byte("\n"))
		if err != nil {
			return fmt.Errorf("fail to print dlq messages: %s", err)
		}
	}
	return nil
}

func (ac *DLQV2Service) PurgeMessages(c *cli.Context) error {
	adminClient := ac.clientFactory.AdminClient(c)
	lastMessageID, err := ac.getLastMessageID(c, "purge")
	if err != nil {
		return err
	}
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.PurgeDLQTasks(ctx, &adminservice.PurgeDLQTasksRequest{
		DlqKey: ac.getDLQKey(),
		InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
			MessageId: lastMessageID,
		},
	})
	if err != nil {
		return fmt.Errorf("call to PurgeDLQTasks failed: %w", err)
	}
	err = newEncoder(ac.writer).Encode(response)
	if err != nil {
		return fmt.Errorf("unable to encode PurgeDLQTasks response: %w", err)
	}
	return nil
}

func (ac *DLQV2Service) MergeMessages(c *cli.Context) error {
	adminClient := ac.clientFactory.AdminClient(c)
	lastMessageID, err := ac.getLastMessageID(c, "merge")
	if err != nil {
		return err
	}
	ctx, cancel := newContext(c)
	defer cancel()

	response, err := adminClient.MergeDLQTasks(ctx, &adminservice.MergeDLQTasksRequest{
		DlqKey: ac.getDLQKey(),
		InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
			MessageId: lastMessageID,
		},
		BatchSize: int32(c.Int(FlagPageSize)), // let the server handle validation and defaulting of batch size.
	})
	if err != nil {
		return fmt.Errorf("call to MergeDLQTasks failed: %w", err)
	}
	err = newEncoder(ac.writer).Encode(response)
	if err != nil {
		return fmt.Errorf("unable to encode MergeDLQTasks response: %w", err)
	}
	return nil
}

func (ac *DLQV2Service) getDLQKey() *commonspb.HistoryDLQKey {
	return &commonspb.HistoryDLQKey{
		TaskCategory:  int32(ac.category.ID()),
		SourceCluster: ac.sourceCluster,
		TargetCluster: ac.targetCluster,
	}
}

func (ac *DLQV2Service) getLastMessageID(c *cli.Context, action string) (int64, error) {
	if !c.IsSet(FlagLastMessageID) {
		msg := fmt.Sprintf(
			"You did not set --%s. Are you sure you want to %s all messages without an upper bound?",
			FlagLastMessageID,
			action,
		)
		ac.prompter.Prompt(msg)
		return persistence.MaxQueueMessageID, nil
	}
	lastMessageID := c.Int64(FlagLastMessageID)
	if lastMessageID < persistence.FirstQueueMessageID {
		return 0, fmt.Errorf(
			"--%s must be at least %d but was %d",
			FlagLastMessageID,
			persistence.FirstQueueMessageID,
			lastMessageID,
		)
	}
	return lastMessageID, nil
}

func getSupportedDLQTaskCategories(taskCategoryRegistry tasks.TaskCategoryRegistry) []tasks.Category {
	categories := make([]tasks.Category, 0, len(taskCategoryRegistry.GetCategories())-1)
	for _, c := range taskCategoryRegistry.GetCategories() {
		if c != tasks.CategoryMemoryTimer {
			categories = append(categories, c)
		}
	}
	slices.SortFunc(categories, func(a, b tasks.Category) int {
		return a.ID() - b.ID()
	})
	return categories
}

func getCategoriesList(taskCategoryRegistry tasks.TaskCategoryRegistry) string {
	var categoryString strings.Builder
	categories := getSupportedDLQTaskCategories(taskCategoryRegistry)
	for i, c := range categories {
		if i == len(categories)-1 {
			categoryString.WriteString(" and ")
		} else if i > 0 {
			categoryString.WriteString(", ")
		}
		_, _ = fmt.Fprintf(&categoryString, "%d (%s)", c.ID(), c.Name())
	}
	return categoryString.String()
}

func getCategoryByID(
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	categoryIDString string,
) (tasks.Category, bool, error) {
	if categoryIDString == "" {
		return tasks.Category{}, false, fmt.Errorf("--%s is required", FlagDLQType)
	}
	id, err := strconv.Atoi(categoryIDString)
	if err != nil {
		return tasks.Category{}, false, fmt.Errorf(
			"%w: unable to parse category ID as an integer: %s", err, categoryIDString,
		)
	}
	for _, c := range getSupportedDLQTaskCategories(taskCategoryRegistry) {
		if c.ID() == id {
			return c, true, nil
		}
	}
	return tasks.Category{}, false, nil
}
