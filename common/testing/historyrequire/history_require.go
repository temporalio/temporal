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
	"google.golang.org/protobuf/proto"
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
//  - ContainsHistoryEvents
//  - WaitForHistoryEvents (call getHistory until expectedHistory is reached, with interval and timeout)
//  - Funcs like WithVersion, WithTime, WithAttributes, WithPayloadLimit(100) and pass them to PrintHistory
//  - oneof support
//  - enums as strings not as ints

func (h HistoryRequire) EqualHistoryEvents(expectedHistory string, actualHistoryEvents []*historypb.HistoryEvent) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedHistoryEvents, expectedEventsAttributes := h.parseHistory(expectedHistory)

	require.Equalf(h.t, len(expectedHistoryEvents), len(actualHistoryEvents), "Length of expected(%d) and actual(%d) histories is not equal", len(expectedHistoryEvents), len(actualHistoryEvents))

	h.equalHistoryEvents(expectedHistoryEvents, expectedEventsAttributes, actualHistoryEvents)
}

func (h HistoryRequire) EqualHistoryEventsSuffix(expectedHistorySuffix string, actualHistoryEvents []*historypb.HistoryEvent) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedHistoryEvents, expectedEventsAttributes := h.parseHistory(expectedHistorySuffix)

	require.GreaterOrEqualf(h.t, len(actualHistoryEvents), len(expectedHistoryEvents), "Length of actual history(%d) must be greater or equal to the length of expected history(%d)", len(actualHistoryEvents), len(expectedHistoryEvents))

	h.equalHistoryEvents(expectedHistoryEvents, expectedEventsAttributes, actualHistoryEvents[len(actualHistoryEvents)-len(expectedHistoryEvents):])
}

func (h HistoryRequire) EqualHistoryEventsPrefix(expectedHistoryPrefix string, actualHistoryEvents []*historypb.HistoryEvent) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedHistoryEvents, expectedEventsAttributes := h.parseHistory(expectedHistoryPrefix)

	require.GreaterOrEqualf(h.t, len(actualHistoryEvents), len(expectedHistoryEvents), "Length of actual history(%d) must be greater or equal to the length of expected history(%d)", len(actualHistoryEvents), len(expectedHistoryEvents))

	h.equalHistoryEvents(expectedHistoryEvents, expectedEventsAttributes, actualHistoryEvents[:len(expectedHistoryEvents)])
}

func (h HistoryRequire) EqualHistory(expectedHistory string, actualHistory *historypb.History) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	h.EqualHistoryEvents(expectedHistory, actualHistory.GetEvents())
}

func (h HistoryRequire) EqualHistorySuffix(expectedHistorySuffix string, actualHistory *historypb.History) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	h.EqualHistoryEventsSuffix(expectedHistorySuffix, actualHistory.GetEvents())
}

func (h HistoryRequire) EqualHistoryPrefix(expectedHistoryPrefix string, actualHistory *historypb.History) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	h.EqualHistoryEventsPrefix(expectedHistoryPrefix, actualHistory.GetEvents())
}

func (h HistoryRequire) PrintHistory(history *historypb.History) {
	h.PrintHistoryEvents(history.GetEvents())
}

func (h HistoryRequire) PrintHistoryEvents(events []*historypb.HistoryEvent) {
	_, _ = fmt.Println(h.formatHistoryEvents(events, false))
}

func (h HistoryRequire) PrintHistoryCompact(history *historypb.History) {
	h.PrintHistoryEventsCompact(history.GetEvents())
}

func (h HistoryRequire) PrintHistoryEventsCompact(events []*historypb.HistoryEvent) {
	_, _ = fmt.Println(h.formatHistoryEvents(events, true))
}

func (h HistoryRequire) equalHistoryEvents(
	expectedHistoryEvents []*historypb.HistoryEvent,
	expectedEventsAttributes []map[string]any,
	actualHistoryEvents []*historypb.HistoryEvent,
) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	var sanitizedActualHistoryEvents []*historypb.HistoryEvent
	for i, actualHistoryEvent := range actualHistoryEvents {
		if expectedHistoryEvents[i].GetEventId() != 0 && expectedHistoryEvents[i].GetVersion() != 0 {
			sanitizedActualHistoryEvents = append(sanitizedActualHistoryEvents, actualHistoryEvent)
			continue
		}

		sanitizedActualHistoryEvent := proto.Clone(actualHistoryEvent).(*historypb.HistoryEvent)
		if expectedHistoryEvents[i].GetEventId() == 0 {
			sanitizedActualHistoryEvent.EventId = 0
		}
		if expectedHistoryEvents[i].GetVersion() == 0 {
			sanitizedActualHistoryEvent.Version = 0
		}
		sanitizedActualHistoryEvents = append(sanitizedActualHistoryEvents, sanitizedActualHistoryEvent)
	}

	expectedCompactHistory := h.formatHistoryEvents(expectedHistoryEvents, true)
	actualCompactHistory := h.formatHistoryEvents(sanitizedActualHistoryEvents, true)
	require.Equal(h.t, expectedCompactHistory, actualCompactHistory)
	for i, actualHistoryEvent := range sanitizedActualHistoryEvents {
		if expectedEventAttributes := expectedEventsAttributes[i]; expectedEventAttributes != nil {
			actualEventAttributes := reflect.ValueOf(actualHistoryEvent.Attributes).Elem().Field(0).Elem()
			h.equalExpectedMapToActualAttributes(expectedEventAttributes, actualEventAttributes, actualHistoryEvent.EventId, "")
		}
	}
}

func (h HistoryRequire) formatHistoryEvents(historyEvents []*historypb.HistoryEvent, compact bool) string {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	if len(historyEvents) == 0 {
		return ""
	}

	var sb strings.Builder
	for _, event := range historyEvents {
		var eventIDStr string
		if event.GetEventId() != 0 {
			eventIDStr = fmt.Sprintf("%3d ", event.GetEventId())
		}

		var versionStr string
		if event.GetVersion() != 0 {
			versionStr = fmt.Sprintf("v%2d ", event.GetVersion())
		}

		var eventAttrsJsonStr string
		if !compact {
			eventAttrs := reflect.ValueOf(event.Attributes).Elem().Field(0).Elem().Interface()
			eventAttrsMap := h.structToMap(eventAttrs)
			eventAttrsJsonBytes, err := json.Marshal(eventAttrsMap)
			require.NoError(h.t, err)
			eventAttrsJsonStr = " " + string(eventAttrsJsonBytes)
		}
		_, _ = sb.WriteString(strings.Join([]string{eventIDStr, versionStr, event.GetEventType().String(), eventAttrsJsonStr, "\n"}, ""))
	}

	return sb.String()[:sb.Len()-1]
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

// parseHistory accept history in a formatHistoryEvents format and return slice of history events w/o attributes and maps of event attributes for every event.
func (h HistoryRequire) parseHistory(history string) ([]*historypb.HistoryEvent, []map[string]any) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	var historyEvents []*historypb.HistoryEvent
	var eventsAttrs []map[string]any
	prevEventID := 0
	firstEvent := true
	for lineNum, eventLine := range strings.Split(history, "\n") {
		fields := strings.Fields(eventLine)
		if len(fields) == 0 || fields[0] == "//" {
			continue
		}
		if len(fields) < 1 {
			require.FailNowf(h.t, "", "Not enough fields on line %d", lineNum+1)
		}

		nextFieldIndex := 0

		eventID, err := strconv.Atoi(fields[nextFieldIndex])
		if err == nil {
			nextFieldIndex++
		}
		if !firstEvent && eventID != 0 && eventID != prevEventID+1 {
			require.FailNowf(h.t, "", "Wrong EventID sequence after EventID %d on line %d", prevEventID, lineNum+1)
		}
		prevEventID = eventID
		firstEvent = false

		var eventVersion int
		if fields[nextFieldIndex][0:1] == "v" {
			eventVersion, err = strconv.Atoi(fields[nextFieldIndex][1:])
			if err != nil {
				require.FailNowf(h.t, "", "Wrong event version %s on line %d. Must be in v1 format", fields[nextFieldIndex], lineNum+1)
			}
			nextFieldIndex++
		}

		eventType, err := enumspb.EventTypeFromString(fields[nextFieldIndex])
		if err != nil {
			require.FailNowf(h.t, "", "Unknown event type %s for event on line %d", fields[nextFieldIndex], lineNum+1)
		}
		historyEvents = append(historyEvents, &historypb.HistoryEvent{
			EventId:   int64(eventID),
			Version:   int64(eventVersion),
			EventType: eventType,
		})
		nextFieldIndex++
		var jb strings.Builder
		for i := nextFieldIndex; i < len(fields); i++ {
			if strings.HasPrefix(fields[i], "//") {
				break
			}
			_, _ = jb.WriteString(fields[i])
			_, _ = jb.WriteRune(' ')
		}
		var eventAttrs map[string]any
		if jb.Len() > 0 {
			err = json.Unmarshal([]byte(jb.String()), &eventAttrs)
			if err != nil {
				require.FailNowf(h.t, err.Error(), "Failed to unmarshal attributes %q for event on line %d", jb.String(), lineNum+1)
			}
		}
		eventsAttrs = append(eventsAttrs, eventAttrs)
	}
	return historyEvents, eventsAttrs
}
