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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/namespace"
)

func prettyPrintJSONObject(c *cli.Context, o interface{}) {
	var b []byte
	var err error
	if pb, ok := o.(proto.Message); ok {
		encoder := codec.NewJSONPBIndentEncoder("  ")
		b, err = encoder.Encode(pb)
	} else {
		b, err = json.MarshalIndent(o, "", "  ")
	}

	if err != nil {
		fmt.Fprintf(c.App.ErrWriter, "Error when trying to pretty-print: %v\n%v\n", err, o)
		return
	}

	_, _ = c.App.Writer.Write(b)
	_, _ = c.App.Writer.Write([]byte("\n"))
}

func getRequiredOption(c *cli.Context, optionName string) (string, error) {
	value := c.String(optionName)
	if len(value) == 0 {
		return "", fmt.Errorf("option is required: %s", optionName)
	}
	return value, nil
}

func parseTime(timeStr string, defaultValue time.Time, now time.Time) (time.Time, error) {
	if len(timeStr) == 0 {
		return defaultValue, nil
	}

	// try to parse
	parsedTime, err := time.Parse(defaultDateTimeFormat, timeStr)
	if err == nil {
		return parsedTime, nil
	}

	// treat as raw unix time
	resultValue, err := strconv.ParseInt(timeStr, 10, 64)
	if err == nil {
		return time.Unix(0, resultValue).UTC(), nil
	}

	// treat as time range format
	parsedTime, err = parseTimeRange(timeStr, now)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse time '%s', use UTC format '2006-01-02T15:04:05', "+
			"time range or raw UnixNano directly. See help for more details: %s", timeStr, err)
	}
	return parsedTime, nil
}

// parseTimeRange parses a given time duration string (in format X<time-duration>) and
// returns parsed timestamp given that duration in the past from current time.
// All valid values must contain a number followed by a time-duration, from the following list (long form/short form):
// - second/s
// - minute/m
// - hour/h
// - day/d
// - week/w
// - month/M
// - year/y
// For example, possible input values, and their result:
// - "3d" or "3day" --> three days --> time.Now().UTC().Add(-3 * 24 * time.Hour)
// - "2m" or "2minute" --> two minutes --> time.Now().UTC().Add(-2 * time.Minute)
// - "1w" or "1week" --> one week --> time.Now().UTC().Add(-7 * 24 * time.Hour)
// - "30s" or "30second" --> thirty seconds --> time.Now().UTC().Add(-30 * time.Second)
// Note: Duration strings are case-sensitive, and should be used as mentioned above only.
// Limitation: Value of numerical multiplier, X should be in b/w 0 - 1e6 (1 million), boundary values excluded i.e.
// 0 < X < 1e6. Also, the maximum time in the past can be 1 January 1970 00:00:00 UTC (epoch time),
// so giving "1000y" will result in epoch time.
func parseTimeRange(timeRange string, now time.Time) (time.Time, error) {
	match, err := regexp.MatchString(defaultDateTimeRangeShortRE, timeRange)
	if !match { // fallback on to check if it's of longer notation
		_, err = regexp.MatchString(defaultDateTimeRangeLongRE, timeRange)
	}
	if err != nil {
		return time.Time{}, err
	}

	re, _ := regexp.Compile(defaultDateTimeRangeNum)
	idx := re.FindStringSubmatchIndex(timeRange)
	if idx == nil {
		return time.Time{}, fmt.Errorf("cannot parse timeRange %s", timeRange)
	}

	num, err := strconv.Atoi(timeRange[idx[0]:idx[1]])
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse timeRange %s", timeRange)
	}
	if num >= 1e6 {
		return time.Time{}, fmt.Errorf("invalid time-duation multiplier %d, allowed range is 0 < multiplier < 1000000", num)
	}

	dur, err := parseTimeDuration(timeRange[idx[1]:])
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse timeRange %s", timeRange)
	}

	res := now.Add(time.Duration(-num) * dur) // using server's local timezone
	epochTime := time.Unix(0, 0).UTC()
	if res.Before(epochTime) {
		res = epochTime
	}
	return res, nil
}

// parseTimeDuration parses the given time duration in either short or long convention
// and returns the time.Duration
// Valid values (long notation/short notation):
// - second/s
// - minute/m
// - hour/h
// - day/d
// - week/w
// - month/M
// - year/y
// NOTE: the input "duration" is case-sensitive
func parseTimeDuration(duration string) (dur time.Duration, err error) {
	switch duration {
	case "s", "second":
		dur = time.Second
	case "m", "minute":
		dur = time.Minute
	case "h", "hour":
		dur = time.Hour
	case "d", "day":
		dur = day
	case "w", "week":
		dur = week
	case "M", "month":
		dur = month
	case "y", "year":
		dur = year
	default:
		err = fmt.Errorf("unknown time duration %s", duration)
	}
	return
}

func newContext(c *cli.Context) (context.Context, context.CancelFunc) {
	return newContextWithTimeout(c, defaultContextTimeout)
}

func newContextWithTimeout(c *cli.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if c.IsSet(FlagContextTimeout) {
		timeout = time.Duration(c.Int(FlagContextTimeout)) * time.Second
	}

	return context.WithTimeout(c.Context, timeout)
}

func StringToEnum(search string, candidates map[string]int32) (int32, error) {
	if search == "" {
		return 0, nil
	}

	var candidateNames []string
	for key, value := range candidates {
		if strings.EqualFold(key, search) {
			return value, nil
		}
		candidateNames = append(candidateNames, key)
	}

	return 0, fmt.Errorf("could not find corresponding candidate for %s. Possible candidates: %q", search, candidateNames)
}

// paginate creates an interactive CLI mode to control the printing of items
func paginate[V any](c *cli.Context, paginationFn collection.PaginationFn[V], pageSize int) error {
	more := c.Bool(FlagMore)
	isTableView := !c.Bool(FlagPrintJSON)
	iter := collection.NewPagingIterator(paginationFn)

	var pageItems []interface{}
	for iter.HasNext() {
		item, err := iter.Next()
		if err != nil {
			return err
		}

		pageItems = append(pageItems, item)
		if len(pageItems) == pageSize || !iter.HasNext() {
			if isTableView {
				if err := printTable(pageItems, c.App.Writer); err != nil {
					return err
				}
			} else {
				prettyPrintJSONObject(c, pageItems)
			}

			if !more || !showNextPage(c.App.Writer) {
				break
			}
			pageItems = pageItems[:0]
		}
	}

	return nil
}

var exportRgx = regexp.MustCompile("^[A-Z]")

func printTable(items []interface{}, writer io.Writer) error {
	if len(items) == 0 {
		return nil
	}

	e := reflect.ValueOf(items[0])
	for e.Type().Kind() == reflect.Ptr {
		e = e.Elem()
	}

	var fields []string
	var exportedFields []int
	t := e.Type()
	for i := 0; i < e.NumField(); i++ {
		if exportRgx.MatchString(t.Field(i).Name) {
			fields = append(fields, t.Field(i).Name)
			exportedFields = append(exportedFields, i)
		}
	}

	table := tablewriter.NewWriter(writer)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	table.SetHeader(fields)
	table.SetHeaderLine(false)
	for i := 0; i < len(items); i++ {
		item := reflect.ValueOf(items[i])
		for item.Type().Kind() == reflect.Ptr {
			item = item.Elem()
		}
		var columns []string
		for _, j := range exportedFields {
			col := item.Field(j)
			columns = append(columns, fmt.Sprintf("%v", col.Interface()))
		}
		table.Append(columns)
	}
	table.Render()
	table.ClearRows()

	return nil
}

func showNextPage(wr io.Writer) bool {
	fmt.Fprintf(wr, "Press %s to show next page, press %s to quit: ",
		color.GreenString("Enter"), color.RedString("any other key then Enter"))
	var input string
	_, _ = fmt.Scanln(&input)
	return strings.Trim(input, " ") == ""
}

func getNamespaceID(c *cli.Context, clientFactory ClientFactory, nsName namespace.Name) (namespace.ID, error) {
	wfClient := clientFactory.WorkflowClient(c)

	ctx, cancel := newContext(c)
	defer cancel()

	nsResponse, err := wfClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: nsName.String(),
	})
	if err != nil {
		return namespace.EmptyID, err
	}

	return namespace.ID(nsResponse.NamespaceInfo.GetId()), nil
}
