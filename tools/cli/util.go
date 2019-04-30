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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/fatih/color"
	s "go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
)

// JSONHistorySerializer is used to encode history event in JSON
type JSONHistorySerializer struct{}

// Serialize serializes history.
func (j *JSONHistorySerializer) Serialize(h *s.History) ([]byte, error) {
	return json.Marshal(h.Events)
}

// Deserialize deserializes history
func (j *JSONHistorySerializer) Deserialize(data []byte) (*s.History, error) {
	var events []*s.HistoryEvent
	err := json.Unmarshal(data, &events)
	if err != nil {
		return nil, err
	}
	return &s.History{Events: events}, nil
}

// GetHistory helper method to iterate over all pages and return complete list of history events
func GetHistory(ctx context.Context, workflowClient client.Client, workflowID, runID string) (*s.History, error) {
	iter := workflowClient.GetWorkflowHistory(ctx, workflowID, runID, false,
		s.HistoryEventFilterTypeAllEvent)
	events := []*s.HistoryEvent{}
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	history := &s.History{}
	history.Events = events
	return history, nil
}

// HistoryEventToString convert HistoryEvent to string
func HistoryEventToString(e *s.HistoryEvent, printFully bool, maxFieldLength int) string {
	data := getEventAttributes(e)
	return anyToString(data, printFully, maxFieldLength)
}

func anyToString(d interface{}, printFully bool, maxFieldLength int) string {
	v := reflect.ValueOf(d)
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
			fieldValue := valueToString(f, printFully, maxFieldLength)
			if len(fieldValue) == 0 {
				continue
			}
			if buf.Len() > 1 {
				buf.WriteString(", ")
			}
			fieldName := t.Field(i).Name
			if !isAttributeName(fieldName) {
				if !printFully {
					fieldValue = trimTextAndBreakWords(fieldValue, maxFieldLength)
				} else if maxFieldLength != 0 { // for command run workflow and observe history
					fieldValue = trimText(fieldValue, maxFieldLength)
				}
			}
			if fieldName == "Reason" || fieldName == "Details" || fieldName == "Cause" {
				buf.WriteString(fmt.Sprintf("%s:%s", color.RedString(fieldName), color.MagentaString(fieldValue)))
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
			switch val.Interface().(type) {
			case []byte:
				str += string(val.Interface().([]byte))
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
func ColorEvent(e *s.HistoryEvent) string {
	var data string
	switch e.GetEventType() {
	case s.EventTypeWorkflowExecutionStarted:
		data = color.BlueString(e.EventType.String())

	case s.EventTypeWorkflowExecutionCompleted:
		data = color.GreenString(e.EventType.String())

	case s.EventTypeWorkflowExecutionFailed:
		data = color.RedString(e.EventType.String())

	case s.EventTypeWorkflowExecutionTimedOut:
		data = color.YellowString(e.EventType.String())

	case s.EventTypeDecisionTaskScheduled:
		data = e.EventType.String()

	case s.EventTypeDecisionTaskStarted:
		data = e.EventType.String()

	case s.EventTypeDecisionTaskCompleted:
		data = e.EventType.String()

	case s.EventTypeDecisionTaskTimedOut:
		data = color.YellowString(e.EventType.String())

	case s.EventTypeActivityTaskScheduled:
		data = e.EventType.String()

	case s.EventTypeActivityTaskStarted:
		data = e.EventType.String()

	case s.EventTypeActivityTaskCompleted:
		data = e.EventType.String()

	case s.EventTypeActivityTaskFailed:
		data = color.RedString(e.EventType.String())

	case s.EventTypeActivityTaskTimedOut:
		data = color.YellowString(e.EventType.String())

	case s.EventTypeActivityTaskCancelRequested:
		data = e.EventType.String()

	case s.EventTypeRequestCancelActivityTaskFailed:
		data = color.RedString(e.EventType.String())

	case s.EventTypeActivityTaskCanceled:
		data = e.EventType.String()

	case s.EventTypeTimerStarted:
		data = e.EventType.String()

	case s.EventTypeTimerFired:
		data = e.EventType.String()

	case s.EventTypeCancelTimerFailed:
		data = color.RedString(e.EventType.String())

	case s.EventTypeTimerCanceled:
		data = color.MagentaString(e.EventType.String())

	case s.EventTypeWorkflowExecutionCancelRequested:
		data = e.EventType.String()

	case s.EventTypeWorkflowExecutionCanceled:
		data = color.MagentaString(e.EventType.String())

	case s.EventTypeRequestCancelExternalWorkflowExecutionInitiated:
		data = e.EventType.String()

	case s.EventTypeRequestCancelExternalWorkflowExecutionFailed:
		data = color.RedString(e.EventType.String())

	case s.EventTypeExternalWorkflowExecutionCancelRequested:
		data = e.EventType.String()

	case s.EventTypeMarkerRecorded:
		data = e.EventType.String()

	case s.EventTypeWorkflowExecutionSignaled:
		data = e.EventType.String()

	case s.EventTypeWorkflowExecutionTerminated:
		data = e.EventType.String()

	case s.EventTypeWorkflowExecutionContinuedAsNew:
		data = e.EventType.String()

	case s.EventTypeStartChildWorkflowExecutionInitiated:
		data = e.EventType.String()

	case s.EventTypeStartChildWorkflowExecutionFailed:
		data = color.RedString(e.EventType.String())

	case s.EventTypeChildWorkflowExecutionStarted:
		data = color.BlueString(e.EventType.String())

	case s.EventTypeChildWorkflowExecutionCompleted:
		data = color.GreenString(e.EventType.String())

	case s.EventTypeChildWorkflowExecutionFailed:
		data = color.RedString(e.EventType.String())

	case s.EventTypeChildWorkflowExecutionCanceled:
		data = color.MagentaString(e.EventType.String())

	case s.EventTypeChildWorkflowExecutionTimedOut:
		data = color.YellowString(e.EventType.String())

	case s.EventTypeChildWorkflowExecutionTerminated:
		data = e.EventType.String()

	case s.EventTypeSignalExternalWorkflowExecutionInitiated:
		data = e.EventType.String()

	case s.EventTypeSignalExternalWorkflowExecutionFailed:
		data = color.RedString(e.EventType.String())

	case s.EventTypeExternalWorkflowExecutionSignaled:
		data = e.EventType.String()

	default:
		data = e.EventType.String()
	}
	return data
}

func getEventAttributes(e *s.HistoryEvent) interface{} {
	var data interface{}
	switch e.GetEventType() {
	case s.EventTypeWorkflowExecutionStarted:
		data = e.WorkflowExecutionStartedEventAttributes

	case s.EventTypeWorkflowExecutionCompleted:
		data = e.WorkflowExecutionCompletedEventAttributes

	case s.EventTypeWorkflowExecutionFailed:
		data = e.WorkflowExecutionFailedEventAttributes

	case s.EventTypeWorkflowExecutionTimedOut:
		data = e.WorkflowExecutionTimedOutEventAttributes

	case s.EventTypeDecisionTaskScheduled:
		data = e.DecisionTaskScheduledEventAttributes

	case s.EventTypeDecisionTaskStarted:
		data = e.DecisionTaskStartedEventAttributes

	case s.EventTypeDecisionTaskCompleted:
		data = e.DecisionTaskCompletedEventAttributes

	case s.EventTypeDecisionTaskTimedOut:
		data = e.DecisionTaskTimedOutEventAttributes

	case s.EventTypeActivityTaskScheduled:
		data = e.ActivityTaskScheduledEventAttributes

	case s.EventTypeActivityTaskStarted:
		data = e.ActivityTaskStartedEventAttributes

	case s.EventTypeActivityTaskCompleted:
		data = e.ActivityTaskCompletedEventAttributes

	case s.EventTypeActivityTaskFailed:
		data = e.ActivityTaskFailedEventAttributes

	case s.EventTypeActivityTaskTimedOut:
		data = e.ActivityTaskTimedOutEventAttributes

	case s.EventTypeActivityTaskCancelRequested:
		data = e.ActivityTaskCancelRequestedEventAttributes

	case s.EventTypeRequestCancelActivityTaskFailed:
		data = e.RequestCancelActivityTaskFailedEventAttributes

	case s.EventTypeActivityTaskCanceled:
		data = e.ActivityTaskCanceledEventAttributes

	case s.EventTypeTimerStarted:
		data = e.TimerStartedEventAttributes

	case s.EventTypeTimerFired:
		data = e.TimerFiredEventAttributes

	case s.EventTypeCancelTimerFailed:
		data = e.CancelTimerFailedEventAttributes

	case s.EventTypeTimerCanceled:
		data = e.TimerCanceledEventAttributes

	case s.EventTypeWorkflowExecutionCancelRequested:
		data = e.WorkflowExecutionCancelRequestedEventAttributes

	case s.EventTypeWorkflowExecutionCanceled:
		data = e.WorkflowExecutionCanceledEventAttributes

	case s.EventTypeRequestCancelExternalWorkflowExecutionInitiated:
		data = e.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes

	case s.EventTypeRequestCancelExternalWorkflowExecutionFailed:
		data = e.RequestCancelExternalWorkflowExecutionFailedEventAttributes

	case s.EventTypeExternalWorkflowExecutionCancelRequested:
		data = e.ExternalWorkflowExecutionCancelRequestedEventAttributes

	case s.EventTypeMarkerRecorded:
		data = e.MarkerRecordedEventAttributes

	case s.EventTypeWorkflowExecutionSignaled:
		data = e.WorkflowExecutionSignaledEventAttributes

	case s.EventTypeWorkflowExecutionTerminated:
		data = e.WorkflowExecutionTerminatedEventAttributes

	case s.EventTypeWorkflowExecutionContinuedAsNew:
		data = e.WorkflowExecutionContinuedAsNewEventAttributes

	case s.EventTypeStartChildWorkflowExecutionInitiated:
		data = e.StartChildWorkflowExecutionInitiatedEventAttributes

	case s.EventTypeStartChildWorkflowExecutionFailed:
		data = e.StartChildWorkflowExecutionFailedEventAttributes

	case s.EventTypeChildWorkflowExecutionStarted:
		data = e.ChildWorkflowExecutionStartedEventAttributes

	case s.EventTypeChildWorkflowExecutionCompleted:
		data = e.ChildWorkflowExecutionCompletedEventAttributes

	case s.EventTypeChildWorkflowExecutionFailed:
		data = e.ChildWorkflowExecutionFailedEventAttributes

	case s.EventTypeChildWorkflowExecutionCanceled:
		data = e.ChildWorkflowExecutionCanceledEventAttributes

	case s.EventTypeChildWorkflowExecutionTimedOut:
		data = e.ChildWorkflowExecutionTimedOutEventAttributes

	case s.EventTypeChildWorkflowExecutionTerminated:
		data = e.ChildWorkflowExecutionTerminatedEventAttributes

	case s.EventTypeSignalExternalWorkflowExecutionInitiated:
		data = e.SignalExternalWorkflowExecutionInitiatedEventAttributes

	case s.EventTypeSignalExternalWorkflowExecutionFailed:
		data = e.SignalExternalWorkflowExecutionFailedEventAttributes

	case s.EventTypeExternalWorkflowExecutionSignaled:
		data = e.ExternalWorkflowExecutionSignaledEventAttributes

	default:
		data = e
	}
	return data
}

func isAttributeName(name string) bool {
	for i := s.EventType(0); i < s.EventType(40); i++ {
		if name == i.String()+"EventAttributes" {
			return true
		}
	}
	return false
}
