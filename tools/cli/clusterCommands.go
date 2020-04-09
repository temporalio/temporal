package cli

import (
	"os"
	"sort"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
)

// GetSearchAttributes get valid search attributes
func GetSearchAttributes(c *cli.Context) {
	wfClient := getWorkflowClientWithOptionalNamespace(c)
	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := wfClient.GetSearchAttributes(ctx)
	if err != nil {
		ErrorAndExit("Failed to get search attributes.", err)
	}

	table := tablewriter.NewWriter(os.Stdout)
	header := []string{"Key", "Value type"}
	table.SetHeader(header)
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue)
	rows := [][]string{}
	for k, v := range resp.Keys {
		rows = append(rows, []string{k, v.String()})
	}
	sort.Sort(byKey(rows))
	table.AppendBulk(rows)
	table.Render()
}
