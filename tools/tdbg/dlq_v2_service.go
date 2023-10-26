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
	"strconv"
	"strings"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slices"

	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type DLQV2Service struct {
	category      tasks.Category
	sourceCluster string
	targetCluster string
	clientFactory ClientFactory
	writer        io.Writer
	marshaler     jsonpb.Marshaler
	prompter      *Prompter
}

const dlqV2DefaultMaxMessageCount = 100

func NewDLQV2Service(
	category tasks.Category,
	sourceCluster string,
	targetCluster string,
	clientFactory ClientFactory,
	writer io.Writer,
	prompter *Prompter,
) *DLQV2Service {
	return &DLQV2Service{
		category:      category,
		sourceCluster: sourceCluster,
		targetCluster: targetCluster,
		clientFactory: clientFactory,
		writer:        writer,
		prompter:      prompter,
		marshaler: jsonpb.Marshaler{
			Indent:       "  ",
			EmitDefaults: true,
		},
	}
}

func (ac *DLQV2Service) ReadMessages(c *cli.Context) error {
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

	outputFile, err := getOutputFile(c.String(FlagOutputFilename))
	if err != nil {
		return err
	}
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
		remainingMessageCount--
		// TODO: decode the task and print it in a human readable format
		taskString, err := ac.marshaler.MarshalToString(dlqTask)
		if err != nil {
			return fmt.Errorf("unable to encode dlq message: %w", err)
		}
		_, err = outputFile.WriteString(fmt.Sprintf("%v\n", taskString))
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
	err = ac.marshaler.Marshal(ac.writer, response)
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
	err = ac.marshaler.Marshal(ac.writer, response)
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
