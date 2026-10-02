package tdbg

import (
	"fmt"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"
)

type compactVisExecutionInfo struct {
	Namespace  string
	WorkflowID string
	RunID      string
	Type       string
	Status     enumspb.WorkflowExecutionStatus
	StartTime  time.Time
	CloseTime  *time.Time
}

func AdminListExecutions(c *cli.Context, clientFactory ClientFactory) error {
	namespace := ""
	if c.IsSet(FlagNamespace) {
		namespace = c.String(FlagNamespace)
	}
	query := c.String(FlagVisibilityQuery)
	pageSize := c.Int(FlagPageSize)
	tableView := !c.Bool(FlagPrintJSON)

	client := clientFactory.AdminClient(c)
	req := &adminservice.ListExecutionsRequest{
		Namespace: namespace,
		Query:     query,
		PageSize:  int32(pageSize),
	}

	paginationFunc := func(paginationToken []byte) ([]any, []byte, error) {
		ctx, cancel := newContext(c)
		defer cancel()

		req.NextPageToken = paginationToken
		response, err := client.ListExecutions(ctx, req)
		if err != nil {
			return nil, nil, err
		}

		var items []any
		for _, execution := range response.GetExecutions() {
			if !tableView {
				items = append(items, execution)
			} else {
				ns := execution.Namespace
				if ns == "" {
					ns = execution.NamespaceId
				}
				items = append(items, compactVisExecutionInfo{
					Namespace:  ns,
					WorkflowID: execution.Execution.WorkflowId,
					RunID:      execution.Execution.RunId,
					Type:       execution.WorkflowType.GetName(),
					Status:     execution.Status,
					StartTime:  execution.StartTime.AsTime(),
					CloseTime:  timestamp.TimeValuePtr(execution.CloseTime),
				})
			}
		}
		return items, response.GetNextPageToken(), nil
	}

	if err := paginate(c, paginationFunc, pageSize); err != nil {
		return fmt.Errorf("unable to list executions: %w", err)
	}

	return nil
}

type countGroup struct {
	Key   string
	Count int64
}

func AdminCountExecutions(c *cli.Context, clientFactory ClientFactory) error {
	namespace := ""
	if c.IsSet(FlagNamespace) {
		namespace = c.String(FlagNamespace)
	}
	query := c.String(FlagVisibilityQuery)

	client := clientFactory.AdminClient(c)
	req := &adminservice.CountExecutionsRequest{
		Namespace: namespace,
		Query:     query,
	}

	ctx, cancel := newContext(c)
	defer cancel()

	response, err := client.CountExecutions(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to count executions: %v", err)
	}

	if len(response.Groups) > 0 {
		items := make([]any, 0, len(response.Groups))
		for _, group := range response.Groups {
			var key []string
			for _, valuePayload := range group.GroupValues {
				var val any
				if err := payload.Decode(valuePayload, &val); err != nil {
					return fmt.Errorf("failed to decode payload: %w", err)
				}
				key = append(key, fmt.Sprintf("%v", val))
			}
			items = append(items, countGroup{
				Key:   strings.Join(key, ","),
				Count: group.Count,
			})
		}

		if err := printTable(items, c.App.Writer); err != nil {
			return fmt.Errorf("unable to display results: %w", err)
		}
	}

	_, _ = fmt.Fprintf(c.App.Writer, "\nTotal Count: %d\n", response.Count)
	return nil
}
