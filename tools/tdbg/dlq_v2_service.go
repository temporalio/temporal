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
	"errors"
	"fmt"
	"math"
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
}

const dlqV2DefaultMaxMessageCount = 100

func NewDLQV2Service(
	category tasks.Category,
	sourceCluster string,
	targetCluster string,
	clientFactory ClientFactory,
) *DLQV2Service {
	return &DLQV2Service{
		category:      category,
		sourceCluster: sourceCluster,
		targetCluster: targetCluster,
		clientFactory: clientFactory,
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
	var maxMessageID int64 = math.MaxInt64
	if c.IsSet(FlagLastMessageID) {
		maxMessageID = c.Int64(FlagLastMessageID)
		if maxMessageID < persistence.FirstQueueMessageID {
			return fmt.Errorf(
				"--%s must be at least %d but was %d",
				FlagLastMessageID,
				persistence.FirstQueueMessageID,
				maxMessageID,
			)
		}
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

	encoder := jsonpb.Marshaler{
		Indent:       "  ",
		EmitDefaults: true,
	}
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
		taskString, err := encoder.MarshalToString(dlqTask)
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

func (ac *DLQV2Service) PurgeMessages(*cli.Context) error {
	return errors.New("purge is not yet implemented for DLQ v2")
}

func (ac *DLQV2Service) MergeMessages(*cli.Context) error {
	return errors.New("merge is not yet implemented for DLQ v2")
}

func getSupportedDLQTaskCategories(taskCategoryRegistry tasks.TaskCategoryRegistry) []tasks.Category {
	categories := make([]tasks.Category, 0, len(taskCategoryRegistry.GetCategories())-1)
	for _, c := range taskCategoryRegistry.GetCategories() {
		if c != tasks.CategoryMemoryTimer {
			categories = append(categories, c)
		}
	}
	slices.SortFunc(categories, func(a, b tasks.Category) int {
		return int(a.ID() - b.ID())
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
			"%w: unable to parse category ID as an integer: %s",
			err,
			categoryIDString,
		)
	}
	for _, c := range getSupportedDLQTaskCategories(taskCategoryRegistry) {
		if c.ID() == id {
			return c, true, nil
		}
	}
	return tasks.Category{}, false, nil
}
