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

package historyrequire

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

type (
	HistoryRequire struct {
		t require.TestingT
	}

	helper interface {
		Helper()
	}
)

var publicRgx = regexp.MustCompile("^[A-Z]")
var typeOfBytes = reflect.TypeOf([]byte(nil))

func New(t require.TestingT) HistoryRequire {
	return HistoryRequire{
		t: t,
	}
}

// TODO (maybe):
//  - ContainsHistoryEvents (should accept expectedHistory with and w/o event Ids)
//  - StartsWithHistoryEvents (should accept expectedHistory with and w/o event Ids)
//  - EndsWithHistoryEvents (should accept expectedHistory with and w/o event Ids)
//  - WaitForHistoryEvents (call getHistory until expectedHistory is found, with interval and timeout)
//  - Funcs like WithVersion, WithTime, WithAttributes, WithPayloadLimit(100) and pass them to PrintHistory
//  - oneof support
//  - enums as strings not as ints

func (h HistoryRequire) EqualHistoryEvents(expectedHistory string, actualHistoryEvents []*historypb.HistoryEvent) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedCompactHistory, expectedEventsAttributes := h.parseHistory(expectedHistory)
	actualCompactHistory := h.formatHistoryEventsCompact(actualHistoryEvents)
	require.Equal(h.t, expectedCompactHistory, actualCompactHistory)
	for _, actualHistoryEvent := range actualHistoryEvents {
		if expectedEventAttributes, ok := expectedEventsAttributes[actualHistoryEvent.EventId]; ok {
			actualEventAttributes := reflect.ValueOf(actualHistoryEvent.Attributes).Elem().Field(0).Elem()
			h.equalExpectedMapToActualAttributes(expectedEventAttributes, actualEventAttributes, actualHistoryEvent.EventId, "")
		}
	}
}

// EqualHistoryEventsAndVersions makes an assertion about the events in history and their failover versions.
// TODO(dan) instead of passing versions slice, support an optional version field after eventId in the event format?
// E.g. `3 2 WorkflowExecutionSignaled`
func (h HistoryRequire) EqualHistoryEventsAndVersions(expectedHistory string, expectedVersions []int, actualHistory []*historypb.HistoryEvent) {
	h.EqualHistoryEvents(expectedHistory, actualHistory)
	require.Equal(h.t, len(expectedVersions), len(actualHistory))
	for i := range expectedVersions {
		require.Equal(h.t, int64(expectedVersions[i]), actualHistory[i].Version)
	}
}

func (h HistoryRequire) EqualHistory(expectedHistory string, actualHistory *historypb.History) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	h.EqualHistoryEvents(expectedHistory, actualHistory.GetEvents())
}

func (h HistoryRequire) PrintHistory(history *historypb.History) {
	h.PrintHistoryEvents(history.GetEvents())
}

func (h HistoryRequire) PrintHistoryEvents(events []*historypb.HistoryEvent) {
	_, _ = fmt.Println(h.formatHistoryEvents(events))
}

func (h HistoryRequire) PrintHistoryCompact(history *historypb.History) {
	h.PrintHistoryEventsCompact(history.GetEvents())
}

func (h HistoryRequire) PrintHistoryEventsCompact(events []*historypb.HistoryEvent) {
	_, _ = fmt.Println(h.formatHistoryEventsCompact(events))
}

func (h HistoryRequire) formatHistoryEventsCompact(historyEvents []*historypb.HistoryEvent) string {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	var sb strings.Builder
	for _, event := range historyEvents {
		_, _ = sb.WriteString(fmt.Sprintf("%3d %s\n", event.GetEventId(), event.GetEventType()))
	}
	if sb.Len() > 0 {
		return sb.String()[:sb.Len()-1]
	}
	return ""
}

func (h HistoryRequire) formatHistoryEvents(historyEvents []*historypb.HistoryEvent) string {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	var sb strings.Builder
	for _, event := range historyEvents {
		eventAttrs := reflect.ValueOf(event.Attributes).Elem().Field(0).Elem().Interface()
		eventAttrsMap := h.structToMap(eventAttrs)
		eventAttrsJson, err := json.Marshal(eventAttrsMap)
		require.NoError(h.t, err)
		_, _ = sb.WriteString(fmt.Sprintf("%3d %s %s\n", event.GetEventId(), event.GetEventType(), string(eventAttrsJson)))
	}
	if sb.Len() > 0 {
		return sb.String()[:sb.Len()-1]
	}
	return ""
}

func (h HistoryRequire) structToMap(strct any) map[string]any {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	strctV := reflect.ValueOf(strct)
	strctT := strctV.Type()

	ret := map[string]any{}

	for i := 0; i < strctV.NumField(); i++ {
		field := strctV.Field(i)
		// Skip unexported members
		if !publicRgx.MatchString(strctT.Field(i).Name) {
			continue
		}
		if field.Kind() == reflect.Pointer && field.IsNil() {
			continue
		}
		fieldName := strctT.Field(i).Name
		ret[fieldName] = h.fieldValue(field)
	}

	return ret
}

//nolint:revive // cognitive complexity 29 (> max enabled 25)
func (h HistoryRequire) fieldValue(field reflect.Value) any {
	if field.Kind() == reflect.Pointer && field.Elem().Kind() == reflect.Struct {
		return h.structToMap(field.Elem().Interface())
	} else if field.Kind() == reflect.Struct {
		return h.structToMap(field.Interface())
	} else if field.Kind() == reflect.Slice && field.Type() != typeOfBytes {
		fvSlice := make([]any, field.Len())
		for i := 0; i < field.Len(); i++ {
			fvSlice[i] = h.fieldValue(field.Index(i))
		}
		return fvSlice
	} else if field.Kind() == reflect.Map {
		mr := field.MapRange()
		fvMap := make(map[string]any)
		for mr.Next() {
			// Only string keyed maps are supported.
			key := mr.Key().Interface().(string)
			v := mr.Value()
			if v.Kind() == reflect.Pointer && v.IsNil() {
				continue
			}
			fvMap[key] = h.fieldValue(v)
		}
		return fvMap
	}

	fieldValue := field.Interface()
	if fieldValueBytes, ok := fieldValue.([]byte); ok {
		fieldValue = string(fieldValueBytes)
	}

	return fieldValue
}

//nolint:revive // cognitive complexity 26 (> max enabled 25)
func (h HistoryRequire) equalExpectedMapToActualAttributes(expectedMap map[string]any, actualAttributesV reflect.Value, eventID int64, attrPrefix string) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	if actualAttributesV.Kind() == reflect.Pointer {
		actualAttributesV = actualAttributesV.Elem()
	}

	for attrName, expectedValue := range expectedMap {
		var actualV reflect.Value
		if actualAttributesV.Kind() == reflect.Map {
			actualV = actualAttributesV.MapIndex(reflect.ValueOf(attrName))
		} else if actualAttributesV.Kind() == reflect.Struct {
			actualV = actualAttributesV.FieldByName(attrName)
		} else {
			if attrPrefix == "" {
				attrPrefix = "."
			}
			require.Failf(h.t, "", "Value of property %s for EventID=%v expected to be struct or map but was of type %s", attrPrefix, eventID, actualAttributesV.Type().String())
		}

		if actualV.Kind() == reflect.Invalid {
			require.Failf(h.t, "", "Expected property %s.%s wasn't found for EventID=%v", attrPrefix, attrName, eventID)
		}

		if es, ok := expectedValue.([]any); ok {
			for i, ei := range es {
				eim := ei.(map[string]any)
				h.equalExpectedMapToActualAttributes(eim, actualV.Index(i), eventID, attrPrefix+"."+attrName+"["+strconv.Itoa(i)+"]")
			}
			continue
		}

		if em, ok := expectedValue.(map[string]any); ok {
			if actualV.IsNil() {
				require.Failf(h.t, "", "Value of property %s.%s for EventID=%v expected to be struct or map but was nil", attrPrefix, attrName, eventID)
			}

			h.equalExpectedMapToActualAttributes(em, actualV, eventID, attrPrefix+"."+attrName)
			continue
		}
		actualValue := actualV.Interface()
		// Actual bytes are expressed as strings in expected history.
		if actualValueBytes, ok := actualValue.([]byte); ok {
			actualValue = string(actualValueBytes)
		}

		require.EqualValues(h.t, expectedValue, actualValue, "Values of %s.%s property are not equal for EventID=%v", attrPrefix, attrName, eventID)
	}
}

// parseHistory accept history in a formatHistoryEvents format and returns compact history string and map of eventID to map of event attributes.
func (h HistoryRequire) parseHistory(expectedHistory string) (string, map[int64]map[string]any) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	var historyEvents []*historypb.HistoryEvent
	eventsAttrs := make(map[int64]map[string]any)
	prevEventID := 0
	for lineNum, eventLine := range strings.Split(expectedHistory, "\n") {
		fields := strings.Fields(eventLine)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 2 {
			require.FailNowf(h.t, "", "Not enough fields on line %d", lineNum+1)
		}
		eventID, err := strconv.Atoi(fields[0])
		if err != nil {
			require.FailNowf(h.t, err.Error(), "Failed to parse EventID on line %d", lineNum+1)
		}
		if eventID != prevEventID+1 && prevEventID != 0 {
			require.FailNowf(h.t, "", "Wrong EventID sequence after EventID %d on line %d", prevEventID, lineNum+1)
		}
		prevEventID = eventID
		eventType, err := enumspb.EventTypeFromString(fields[1])
		if err != nil {
			require.FailNowf(h.t, "", "Unknown event type %s for EventID=%d", fields[1], lineNum+1)
		}
		historyEvents = append(historyEvents, &historypb.HistoryEvent{
			EventId:   int64(eventID),
			EventType: eventType,
		})
		var jb strings.Builder
		for i := 2; i < len(fields); i++ {
			if strings.HasPrefix(fields[i], "//") {
				break
			}
			_, _ = jb.WriteString(fields[i])
			_, _ = jb.WriteRune(' ')
		}
		if jb.Len() > 0 {
			var eventAttrs map[string]any
			err := json.Unmarshal([]byte(jb.String()), &eventAttrs)
			if err != nil {
				require.FailNowf(h.t, err.Error(), "Failed to unmarshal attributes %q for EventID=%d", jb.String(), lineNum+1)
			}
			eventsAttrs[int64(eventID)] = eventAttrs
		}
	}
	return h.formatHistoryEventsCompact(historyEvents), eventsAttrs
}
