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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/gogo/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
	"github.com/valyala/fastjson"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkclient "go.temporal.io/sdk/client"

	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/rpc"
)

// GetHistory helper method to iterate over all pages and return complete list of history events
func GetHistory(ctx context.Context, workflowClient sdkclient.Client, workflowID, runID string) (*historypb.History, error) {
	iter := workflowClient.GetWorkflowHistory(ctx, workflowID, runID, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var events []*historypb.HistoryEvent
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	history := &historypb.History{}
	history.Events = events
	return history, nil
}

// HistoryEventToString convert HistoryEvent to string
func HistoryEventToString(e *historypb.HistoryEvent, printFully bool, maxFieldLength int) string {
	data := getEventAttributes(e)
	return anyToString(data, printFully, maxFieldLength)
}

func anyToString(d interface{}, printFully bool, maxFieldLength int) string {
	v := reflect.ValueOf(d)
	// Time is handled separetely because we don't want to go through it fields.
	if t, isTime := d.(time.Time); isTime {
		if t.IsZero() {
			return "<zero>"
		}
		return t.String()
	}
	switch v.Kind() {
	case reflect.Ptr:
		return anyToString(v.Elem().Interface(), printFully, maxFieldLength)
	case reflect.Struct:
		var buf bytes.Buffer
		t := reflect.TypeOf(d)
		buf.WriteString("{")
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.Kind() == reflect.Invalid {
				continue
			}
			fieldName := t.Field(i).Name

			// Filter out private fields.
			if !f.CanInterface() {
				continue
			}

			fieldValue := valueToString(f, printFully, maxFieldLength)
			if fieldValue == "" {
				continue
			}
			if buf.Len() > 1 {
				buf.WriteString(", ")
			}
			if !isAttributeName(fieldName) && !strings.HasSuffix(fieldName, "Failure") {
				if !printFully {
					fieldValue = trimTextAndBreakWords(fieldValue, maxFieldLength)
				} else if maxFieldLength != 0 { // for command run workflow and observe history
					fieldValue = trimText(fieldValue, maxFieldLength)
				}
			}
			if fieldName == "Reason" ||
				fieldName == "Cause" ||
				strings.HasSuffix(fieldName, "Details") {
				buf.WriteString(fmt.Sprintf("%s:%s", color.MagentaString(fieldName), fieldValue))
			} else if strings.HasSuffix(fieldName, "Failure") {
				buf.WriteString(fmt.Sprintf("%s:%s", color.RedString(fieldName), fieldValue))
			} else {
				buf.WriteString(fmt.Sprintf("%s:%s", fieldName, fieldValue))
			}
		}
		buf.WriteString("}")
		return buf.String()
	default:
		return fmt.Sprint(d)
	}
}

func valueToString(v reflect.Value, printFully bool, maxFieldLength int) string {
	switch v.Kind() {
	case reflect.Ptr:
		if ps, isPayloads := v.Interface().(*commonpb.Payloads); isPayloads {
			return payloads.ToString(ps)
		}
		return valueToString(v.Elem(), printFully, maxFieldLength)
	case reflect.Struct:
		return anyToString(v.Interface(), printFully, maxFieldLength)
	case reflect.Invalid:
		return ""
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			n := string(v.Bytes())
			if n != "" && n[len(n)-1] == '\n' {
				return fmt.Sprintf("[%v]", n[:len(n)-1])
			}
			return fmt.Sprintf("[%v]", n)
		}
		return fmt.Sprintf("[len=%d]", v.Len())
	case reflect.Map:
		str := "map{"
		for i, key := range v.MapKeys() {
			str += key.String() + ":"
			val := v.MapIndex(key)
			switch typedV := val.Interface().(type) {
			case []byte:
				str += string(typedV)
			case *commonpb.Payload:
				str += payload.ToString(typedV)
			case *commonpb.Payloads:
				for _, value := range typedV.GetPayloads() {
					str += payload.ToString(value)
				}
			default:
				str += val.String()
			}
			if i != len(v.MapKeys())-1 {
				str += ", "
			}
		}
		str += "}"
		return str
	default:
		return fmt.Sprint(v.Interface())
	}
}

// limit the maximum length for each field
func trimText(input string, maxFieldLength int) string {
	if len(input) > maxFieldLength {
		input = fmt.Sprintf("%s ... %s", input[:maxFieldLength/2], input[(len(input)-maxFieldLength/2):])
	}
	return input
}

// limit the maximum length for each field, and break long words for table item correctly wrap words
func trimTextAndBreakWords(input string, maxFieldLength int) string {
	input = trimText(input, maxFieldLength)
	return breakLongWords(input, maxWordLength)
}

// long words will make output in table cell looks bad,
// break long text "ltltltltllt..." to "ltlt ltlt lt..." will make use of table autowrap so that output is pretty.
func breakLongWords(input string, maxWordLength int) string {
	if len(input) <= maxWordLength {
		return input
	}

	cnt := 0
	for i := 0; i < len(input); i++ {
		if cnt == maxWordLength {
			cnt = 0
			input = input[:i] + " " + input[i:]
			continue
		}
		cnt++
		if input[i] == ' ' {
			cnt = 0
		}
	}
	return input
}

// ColorEvent takes an event and return string with color
// Event with color mapping rules:
//   Failed - red
//   Timeout - yellow
//   Canceled - magenta
//   Completed - green
//   Started - blue
//   Others - default (white/black)
func ColorEvent(e *historypb.HistoryEvent) string {
	var data string
	switch e.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		data = color.BlueString(e.EventType.String())

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		data = color.GreenString(e.EventType.String())

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		data = color.RedString(e.EventType.String())

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		data = color.YellowString(e.EventType.String())

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
		data = color.YellowString(e.EventType.String())

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		data = color.RedString(e.EventType.String())

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		data = color.YellowString(e.EventType.String())

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_TIMER_STARTED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_TIMER_FIRED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_TIMER_CANCELED:
		data = color.MagentaString(e.EventType.String())

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		data = color.MagentaString(e.EventType.String())

	case enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
		data = color.RedString(e.EventType.String())

	case enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_MARKER_RECORDED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
		data = color.RedString(e.EventType.String())

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
		data = color.BlueString(e.EventType.String())

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
		data = color.GreenString(e.EventType.String())

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
		data = color.RedString(e.EventType.String())

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
		data = color.MagentaString(e.EventType.String())

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
		data = color.YellowString(e.EventType.String())

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
		data = color.RedString(e.EventType.String())

	case enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
		data = e.EventType.String()

	case enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		data = e.EventType.String()

	default:
		data = e.EventType.String()
	}
	return data
}

func getEventAttributes(e *historypb.HistoryEvent) interface{} {
	var data interface{}
	switch e.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		data = e.GetWorkflowExecutionStartedEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		data = e.GetWorkflowExecutionCompletedEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		data = e.GetWorkflowExecutionFailedEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		data = e.GetWorkflowExecutionTimedOutEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
		data = e.GetWorkflowTaskScheduledEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
		data = e.GetWorkflowTaskStartedEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
		data = e.GetWorkflowTaskCompletedEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
		data = e.GetWorkflowTaskTimedOutEventAttributes()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		data = e.GetActivityTaskScheduledEventAttributes()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
		data = e.GetActivityTaskStartedEventAttributes()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		data = e.GetActivityTaskCompletedEventAttributes()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		data = e.GetActivityTaskFailedEventAttributes()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		data = e.GetActivityTaskTimedOutEventAttributes()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
		data = e.GetActivityTaskCancelRequestedEventAttributes()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		data = e.GetActivityTaskCanceledEventAttributes()

	case enumspb.EVENT_TYPE_TIMER_STARTED:
		data = e.GetTimerStartedEventAttributes()

	case enumspb.EVENT_TYPE_TIMER_FIRED:
		data = e.GetTimerFiredEventAttributes()

	case enumspb.EVENT_TYPE_TIMER_CANCELED:
		data = e.GetTimerCanceledEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
		data = e.GetWorkflowExecutionCancelRequestedEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		data = e.GetWorkflowExecutionCanceledEventAttributes()

	case enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
		data = e.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()

	case enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
		data = e.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes()

	case enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
		data = e.GetExternalWorkflowExecutionCancelRequestedEventAttributes()

	case enumspb.EVENT_TYPE_MARKER_RECORDED:
		data = e.GetMarkerRecordedEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		data = e.GetWorkflowExecutionSignaledEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		data = e.GetWorkflowExecutionTerminatedEventAttributes()

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		data = e.GetWorkflowExecutionContinuedAsNewEventAttributes()

	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
		data = e.GetStartChildWorkflowExecutionInitiatedEventAttributes()

	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
		data = e.GetStartChildWorkflowExecutionFailedEventAttributes()

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
		data = e.GetChildWorkflowExecutionStartedEventAttributes()

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
		data = e.GetChildWorkflowExecutionCompletedEventAttributes()

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
		data = e.GetChildWorkflowExecutionFailedEventAttributes()

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
		data = e.GetChildWorkflowExecutionCanceledEventAttributes()

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
		data = e.GetChildWorkflowExecutionTimedOutEventAttributes()

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
		data = e.GetChildWorkflowExecutionTerminatedEventAttributes()

	case enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
		data = e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	case enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
		data = e.GetSignalExternalWorkflowExecutionFailedEventAttributes()

	case enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
		data = e.GetExternalWorkflowExecutionSignaledEventAttributes()

	default:
		data = e
	}
	return data
}

func isAttributeName(name string) bool {
	eventType := strings.TrimSuffix(name, "EventAttributes")

	if _, ok := enumspb.EventType_value[eventType]; ok {
		return true
	}

	return false
}

func getCurrentUserFromEnv() string {
	for _, n := range envKeysForUserName {
		if len(os.Getenv(n)) > 0 {
			return os.Getenv(n)
		}
	}
	return "unkown"
}

func prettyPrintJSONObject(o interface{}) {
	var b []byte
	var err error
	if pb, ok := o.(proto.Message); ok {
		encoder := codec.NewJSONPBIndentEncoder("  ")
		b, err = encoder.Encode(pb)
	} else {
		b, err = json.MarshalIndent(o, "", "  ")
	}

	if err != nil {
		fmt.Printf("Error when try to print pretty: %v\n", err)
		fmt.Println(o)
	}
	_, _ = os.Stdout.Write(b)
	fmt.Println()
}

func mapKeysToArray(m map[string]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func printError(msg string, err error) {
	if err != nil {
		fmt.Printf("%s %s\n%s %+v\n", colorRed("Error:"), msg, colorMagenta("Error Details:"), err)
		if os.Getenv(showErrorStackEnv) != `` {
			fmt.Printf("Stack trace:\n")
			debug.PrintStack()
		} else {
			fmt.Printf("('export %s=1' to see stack traces)\n", showErrorStackEnv)
		}
	} else {
		fmt.Printf("%s %s\n", colorRed("Error:"), msg)
	}
}

// ErrorAndExit print easy to understand error msg first then error detail in a new line
func ErrorAndExit(msg string, err error) {
	printError(msg, err)
	osExit(1)
}

func getWorkflowClient(c *cli.Context) sdkclient.Client {
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	return cFactory.SDKClient(c, namespace)
}

func getWorkflowClientWithOptionalNamespace(c *cli.Context) sdkclient.Client {
	if !c.GlobalIsSet(FlagNamespace) {
		_ = c.GlobalSet(FlagNamespace, "system-namespace")
	}
	return getWorkflowClient(c)
}

func getRequiredOption(c *cli.Context, optionName string) string {
	value := c.String(optionName)
	if len(value) == 0 {
		ErrorAndExit(fmt.Sprintf("Option %s is required", optionName), nil)
	}
	return value
}

func getRequiredInt64Option(c *cli.Context, optionName string) int64 {
	if !c.IsSet(optionName) {
		ErrorAndExit(fmt.Sprintf("Option %s is required", optionName), nil)
	}
	return c.Int64(optionName)
}

func getRequiredIntOption(c *cli.Context, optionName string) int {
	if !c.IsSet(optionName) {
		ErrorAndExit(fmt.Sprintf("Option %s is required", optionName), nil)
	}
	return c.Int(optionName)
}

func getRequiredGlobalOption(c *cli.Context, optionName string) string {
	value := c.GlobalString(optionName)
	if len(value) == 0 {
		ErrorAndExit(fmt.Sprintf("Global option %s is required", optionName), nil)
	}
	return value
}

func formatTime(t time.Time, onlyTime bool) string {
	var result string
	if onlyTime {
		result = t.Format(defaultTimeFormat)
	} else {
		result = t.Format(defaultDateTimeFormat)
	}
	return result
}

func parseTime(timeStr string, defaultValue time.Time, now time.Time) time.Time {
	if len(timeStr) == 0 {
		return defaultValue
	}

	// try to parse
	parsedTime, err := time.Parse(defaultDateTimeFormat, timeStr)
	if err == nil {
		return parsedTime
	}

	// treat as raw unix time
	resultValue, err := strconv.ParseInt(timeStr, 10, 64)
	if err == nil {
		return time.Unix(0, resultValue).UTC()
	}

	// treat as time range format
	parsedTime, err = parseTimeRange(timeStr, now)
	if err != nil {
		ErrorAndExit(fmt.Sprintf("Cannot parse time '%s', use UTC format '2006-01-02T15:04:05', "+
			"time range or raw UnixNano directly. See help for more details.", timeStr), err)
	}
	return parsedTime
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
		match, err = regexp.MatchString(defaultDateTimeRangeLongRE, timeRange)
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

func strToTaskQueueType(str string) enumspb.TaskQueueType {
	if strings.ToLower(str) == "activity" {
		return enumspb.TASK_QUEUE_TYPE_ACTIVITY
	}
	return enumspb.TASK_QUEUE_TYPE_WORKFLOW
}

func getCliIdentity() string {
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "UnKnown"
	}
	return fmt.Sprintf("tctl@%s", hostName)
}

func newContext(c *cli.Context) (context.Context, context.CancelFunc) {
	return newContextWithTimeout(c, defaultContextTimeout)
}

func newContextForLongPoll(c *cli.Context) (context.Context, context.CancelFunc) {
	return newContextWithTimeout(c, defaultContextTimeoutForLongPoll)
}

func newIndefiniteContext(c *cli.Context) (context.Context, context.CancelFunc) {
	if c.GlobalIsSet(FlagContextTimeout) {
		timeout := time.Duration(c.GlobalInt(FlagContextTimeout)) * time.Second
		return rpc.NewContextWithTimeoutAndCLIHeaders(timeout)
	}

	return rpc.NewContextWithCLIHeaders()
}

func newContextWithTimeout(c *cli.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if c.GlobalIsSet(FlagContextTimeout) {
		timeout = time.Duration(c.GlobalInt(FlagContextTimeout)) * time.Second
	}

	return rpc.NewContextWithTimeoutAndCLIHeaders(timeout)
}

// process and validate input provided through cmd or file
func processJSONInput(c *cli.Context) *commonpb.Payloads {
	jsonsRaw := readJSONInputs(c, jsonTypeInput)

	var jsons []interface{}
	for _, jsonRaw := range jsonsRaw {
		if jsonRaw == nil {
			jsons = append(jsons, nil)
		} else {
			var j interface{}
			if err := json.Unmarshal(jsonRaw, &j); err != nil {
				ErrorAndExit("Input is not a valid JSON.", err)
			}
			jsons = append(jsons, j)
		}

	}
	p, err := payloads.Encode(jsons...)
	if err != nil {
		ErrorAndExit("Unable to encode input.", err)
	}

	return p
}

// read multiple inputs presented in json format
func readJSONInputs(c *cli.Context, jType jsonType) [][]byte {
	var flagRawInput string
	var flagInputFileName string

	switch jType {
	case jsonTypeInput:
		flagRawInput = FlagInput
		flagInputFileName = FlagInputFile
	case jsonTypeMemo:
		flagRawInput = FlagMemo
		flagInputFileName = FlagMemoFile
	default:
		return nil
	}

	if c.IsSet(flagRawInput) {
		inputsG := c.Generic(flagRawInput)

		var inputs *cli.StringSlice
		var ok bool
		if inputs, ok = inputsG.(*cli.StringSlice); !ok {
			// input could be provided as StringFlag instead of StringSliceFlag
			ss := make(cli.StringSlice, 1)
			ss[0] = fmt.Sprintf("%v", inputsG)
			inputs = &ss
		}

		var inputsRaw [][]byte
		for _, i := range *inputs {
			if strings.EqualFold(i, "null") {
				inputsRaw = append(inputsRaw, []byte(nil))
			} else {
				inputsRaw = append(inputsRaw, []byte(i))
			}
		}

		return inputsRaw
	} else if c.IsSet(flagInputFileName) {
		inputFile := c.String(flagInputFileName)
		// This method is purely used to parse input from the CLI. The input comes from a trusted user
		// #nosec
		data, err := ioutil.ReadFile(inputFile)
		if err != nil {
			ErrorAndExit("Error reading input file", err)
		}
		return [][]byte{data}
	}
	return nil
}

// validate whether str is a valid json or multi valid json concatenated with spaces/newlines
func validateJSONs(str string) error {
	input := []byte(str)
	dec := json.NewDecoder(bytes.NewReader(input))
	for {
		_, err := dec.Token()
		if err == io.EOF {
			return nil // End of input, valid JSON
		}
		if err != nil {
			return err // Invalid input
		}
	}
}

// use parseBool to ensure all BOOL search attributes only be "true" or "false"
func parseBool(str string) (bool, error) {
	switch str {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, fmt.Errorf("not parseable bool value: %s", str)
}

func trimSpace(strs []string) []string {
	result := make([]string, len(strs))
	for i, v := range strs {
		result[i] = strings.TrimSpace(v)
	}
	return result
}

func parseArray(v string) (interface{}, error) {
	if len(v) > 0 && v[0] == '[' && v[len(v)-1] == ']' {
		parsedValues, err := fastjson.Parse(v)
		if err != nil {
			return nil, err
		}
		arr, err := parsedValues.Array()
		if err != nil {
			return nil, err
		}
		result := make([]interface{}, len(arr))
		for i, item := range arr {
			s := item.String()
			if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' { // remove addition quote from json
				s = s[1 : len(s)-1]
				if sTime, err := time.Parse(defaultDateTimeFormat, s); err == nil {
					result[i] = sTime
					continue
				}
			}
			result[i] = s
		}
		return result, nil
	}
	return nil, errors.New("not array")
}

func convertStringToRealType(v string) interface{} {
	var genVal interface{}
	var err error

	if genVal, err = strconv.ParseInt(v, 10, 64); err == nil {

	} else if genVal, err = parseBool(v); err == nil {

	} else if genVal, err = strconv.ParseFloat(v, 64); err == nil {

	} else if genVal, err = time.Parse(defaultDateTimeFormat, v); err == nil {

	} else if genVal, err = parseArray(v); err == nil {

	} else {
		genVal = v
	}

	return genVal
}

func truncate(str string) string {
	if len(str) > maxOutputStringLength {
		return str[:maxOutputStringLength]
	}
	return str
}

// this only works for ANSI terminal, which means remove existing lines won't work if users redirect to file
// ref: https://en.wikipedia.org/wiki/ANSI_escape_code
func removePrevious2LinesFromTerminal() {
	fmt.Printf("\033[1A")
	fmt.Printf("\033[2K")
	fmt.Printf("\033[1A")
	fmt.Printf("\033[2K")
}

func showNextPage() bool {
	fmt.Printf("Press %s to show next page, press %s to quit: ",
		color.GreenString("Enter"), color.RedString("any other key then Enter"))
	var input string
	_, _ = fmt.Scanln(&input)
	return strings.Trim(input, " ") == ""
}

// paginate creates an interactive CLI mode to control the printing of items
func paginate(c *cli.Context, paginationFn collection.PaginationFn) error {
	more := c.Bool(FlagMore)
	isTableView := !c.Bool(FlagPrintJSON)
	pageSize := c.Int(FlagPageSize)
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
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
				printTable(pageItems)
			} else {
				prettyPrintJSONObject(pageItems)
			}

			if !more || !showNextPage() {
				break
			}
			pageItems = pageItems[:0]
		}
	}

	return nil
}

func printTable(items []interface{}) error {
	if len(items) == 0 {
		return nil
	}

	e := reflect.ValueOf(items[0])
	for e.Type().Kind() == reflect.Ptr {
		e = e.Elem()
	}

	var fields []string
	t := e.Type()
	for i := 0; i < e.NumField(); i++ {
		fields = append(fields, t.Field(i).Name)
	}

	table := tablewriter.NewWriter(os.Stdout)
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
		for j := 0; j < len(fields); j++ {
			col := item.Field(j)
			columns = append(columns, fmt.Sprintf("%v", col.Interface()))
		}
		table.Append(columns)
	}
	table.Render()
	table.ClearRows()

	return nil
}

func stringToEnum(search string, candidates map[string]int32) (int32, error) {
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

	return 0, fmt.Errorf("Could not find corresponding candidate for %s. Possible candidates: %q", search, candidateNames)
}

// prompt will show input msg, then waiting user input y/yes to continue
func prompt(msg string, autoConfirm bool) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(msg, " ")
	var text string
	if autoConfirm {
		text = "y"
		fmt.Print("y")
	} else {
		text, _ = reader.ReadString('\n')
	}
	fmt.Println()

	textLower := strings.ToLower(strings.TrimRight(text, "\n"))
	if textLower != "y" && textLower != "yes" {
		os.Exit(0)
	}
}
