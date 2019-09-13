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
	"os"
	"sort"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
)

// GetSearchAttributes get valid search attributes
func GetSearchAttributes(c *cli.Context) {
	wfClient := getWorkflowClientWithOptionalDomain(c)
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
