package historyrequire

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"
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

	HistoryEventsReader func() []*historypb.HistoryEvent
	HistoryReader       func() *historypb.History
)

var (
	publicRgx   = regexp.MustCompile("^[A-Z]")
	typeOfBytes = reflect.TypeOf([]byte(nil))
)

func New(t require.TestingT) HistoryRequire {
	return HistoryRequire{
		t: t,
	}
}

// TODO (maybe):
//  - Funcs like WithTime, WithAttributes, WithPayloadLimit(100) and pass them to PrintHistory
//  - oneof support
//  - enums as strings not as ints

func (h HistoryRequire) EqualHistoryEvents(expectedHistory string, actualHistoryEvents []*historypb.HistoryEvent) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedHistoryEvents, expectedEventsAttributes := h.parseHistory(expectedHistory)

	require.Equalf(h.t, len(expectedHistoryEvents), len(actualHistoryEvents),
		"Length of expected(%d) and actual(%d) histories is not equal - actual history: \n%v",
		len(expectedHistoryEvents), len(actualHistoryEvents), h.formatHistoryEvents(actualHistoryEvents, true))

	h.equalHistoryEvents(expectedHistoryEvents, expectedEventsAttributes, actualHistoryEvents)
}

func (h HistoryRequire) EqualHistoryEventsSuffix(expectedHistorySuffix string, actualHistoryEvents []*historypb.HistoryEvent) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedHistoryEvents, expectedEventsAttributes := h.parseHistory(expectedHistorySuffix)

	require.GreaterOrEqualf(h.t, len(actualHistoryEvents), len(expectedHistoryEvents),
		"Length of actual history(%d) must be greater or equal to the length of expected history suffix(%d) - actual history: \n%v",
		len(actualHistoryEvents), len(expectedHistoryEvents), h.formatHistoryEvents(actualHistoryEvents, true))

	h.equalHistoryEvents(expectedHistoryEvents, expectedEventsAttributes, actualHistoryEvents[len(actualHistoryEvents)-len(expectedHistoryEvents):])
}

func (h HistoryRequire) EqualHistoryEventsPrefix(expectedHistoryPrefix string, actualHistoryEvents []*historypb.HistoryEvent) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedHistoryEvents, expectedEventsAttributes := h.parseHistory(expectedHistoryPrefix)

	require.GreaterOrEqualf(h.t, len(actualHistoryEvents), len(expectedHistoryEvents),
		"Length of actual history(%d) must be greater or equal to the length of expected history prefix(%d) - actual history: \n%v",
		len(actualHistoryEvents), len(expectedHistoryEvents), h.formatHistoryEvents(actualHistoryEvents, true))

	h.equalHistoryEvents(expectedHistoryEvents, expectedEventsAttributes, actualHistoryEvents[:len(expectedHistoryEvents)])
}

// ContainsHistoryEvents checks if expectedHistorySegment is contained in actualHistoryEvents.
// actualHistoryEvents are sanitized based on the first event in expectedHistorySegment.
func (h HistoryRequire) ContainsHistoryEvents(expectedHistorySegment string, actualHistoryEvents []*historypb.HistoryEvent) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedHistoryEvents, expectedEventsAttributes := h.parseHistory(expectedHistorySegment)

	require.GreaterOrEqualf(h.t, len(actualHistoryEvents), len(expectedHistoryEvents),
		"Length of actual history(%d) must be greater or equal to the length of expected history segment(%d) - actual history: \n%v",
		len(actualHistoryEvents), len(expectedHistoryEvents), h.formatHistoryEvents(actualHistoryEvents, true))

	h.containsHistoryEvents(expectedHistoryEvents, expectedEventsAttributes, actualHistoryEvents)
}

func (h HistoryRequire) WaitForHistoryEvents(expectedHistory string, actualHistoryEventsReader HistoryEventsReader, waitFor time.Duration, tick time.Duration) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedHistoryEvents, expectedEventsAttributes := h.parseHistory(expectedHistory)

	var actualHistoryEvents []*historypb.HistoryEvent
	require.EventuallyWithT(h.t, func(t *assert.CollectT) {
		actualHistoryEvents = actualHistoryEventsReader()
		require.Equalf(t, len(expectedHistoryEvents), len(actualHistoryEvents),
			"Length of expected(%d) and actual(%d) histories is not equal - actual history: \n%v",
			len(expectedHistoryEvents), len(actualHistoryEvents), h.formatHistoryEvents(actualHistoryEvents, true))
	}, waitFor, tick)

	h.equalHistoryEvents(expectedHistoryEvents, expectedEventsAttributes, actualHistoryEvents)
}

func (h HistoryRequire) WaitForHistoryEventsSuffix(expectedHistorySuffix string, actualHistoryEventsReader HistoryEventsReader, waitFor time.Duration, tick time.Duration) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedHistoryEvents, expectedEventsAttributes := h.parseHistory(expectedHistorySuffix)

	expectedCompactHistory := h.formatHistoryEvents(expectedHistoryEvents, true)

	var actualHistoryEvents []*historypb.HistoryEvent
	require.EventuallyWithT(h.t, func(t *assert.CollectT) {
		actualHistoryEvents = actualHistoryEventsReader()

		require.GreaterOrEqualf(t, len(actualHistoryEvents), len(expectedHistoryEvents),
			"Length of actual history(%d) must be greater or equal to the length of expected history suffix(%d) - actual history: \n%v",
			len(actualHistoryEvents), len(expectedHistoryEvents), h.formatHistoryEvents(actualHistoryEvents, true))

		actualHistoryEvents = actualHistoryEvents[len(actualHistoryEvents)-len(expectedHistoryEvents):]
		actualHistoryEvents = h.sanitizeActualHistoryEventsForEquals(expectedHistoryEvents, actualHistoryEvents)
		actualCompactHistory := h.formatHistoryEvents(actualHistoryEvents, true)

		require.Equalf(t, expectedCompactHistory, actualCompactHistory,
			"Expected history suffix is not found in actual history. Expected suffix:\n%s\nLast actual:\n%s",
			expectedCompactHistory, actualCompactHistory)
	}, waitFor, tick)

	// TODO: Now if expected sequence of events is reached, all attributes must match.
	//  If attributes values also need to be reached (but not just asserted) then this call
	//  needs to be moved to Eventually block. This will require passing `t`
	//  all way down and replace `require` with `assert`.
	h.equalHistoryEventsAttributes(expectedEventsAttributes, actualHistoryEvents)
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

func (h HistoryRequire) ContainsHistory(expectedHistorySegment string, actualHistory *historypb.History) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	h.ContainsHistoryEvents(expectedHistorySegment, actualHistory.GetEvents())
}

func (h HistoryRequire) WaitForHistory(expectedHistory string, actualHistoryReader HistoryReader, waitFor time.Duration, tick time.Duration) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	h.WaitForHistoryEvents(expectedHistory, func() []*historypb.HistoryEvent {
		return actualHistoryReader().GetEvents()
	}, waitFor, tick)
}

func (h HistoryRequire) WaitForHistorySuffix(expectedHistorySuffix string, actualHistoryReader HistoryReader, waitFor time.Duration, tick time.Duration) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	h.WaitForHistoryEventsSuffix(expectedHistorySuffix, func() []*historypb.HistoryEvent {
		return actualHistoryReader().GetEvents()
	}, waitFor, tick)
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

	actualHistoryEvents = h.sanitizeActualHistoryEventsForEquals(expectedHistoryEvents, actualHistoryEvents)

	expectedCompactHistory := h.formatHistoryEvents(expectedHistoryEvents, true)
	actualCompactHistory := h.formatHistoryEvents(actualHistoryEvents, true)

	require.Equal(h.t, expectedCompactHistory, actualCompactHistory)
	h.equalHistoryEventsAttributes(expectedEventsAttributes, actualHistoryEvents)
}

func (h HistoryRequire) containsHistoryEvents(
	expectedHistoryEvents []*historypb.HistoryEvent,
	expectedEventsAttributes []map[string]any,
	actualHistoryEvents []*historypb.HistoryEvent,
) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	actualHistoryEvents = h.sanitizeActualHistoryEventsForContains(expectedHistoryEvents, actualHistoryEvents)

	expectedCompactHistory := h.formatHistoryEvents(expectedHistoryEvents, true)
	actualCompactHistory := h.formatHistoryEvents(actualHistoryEvents, true)

	startPos := strings.Index(actualCompactHistory, expectedCompactHistory)
	require.True(h.t, startPos >= 0, "Expected history is not found in actual history. Expected:\n%s\nActual:\n%s", expectedCompactHistory, actualCompactHistory)
	actualHistoryEventsFirstIndex := strings.Count(actualCompactHistory[:startPos], "\n")

	h.equalHistoryEventsAttributes(expectedEventsAttributes, actualHistoryEvents[actualHistoryEventsFirstIndex:])
}

// sanitizeActualHistoryEventsForEquals clears EventID and Version fields from actual history events
// if they are not set in the CORRESPONDING expected history event.
// The length of expectedHistoryEvents and actualHistoryEvents must be equal.
func (h HistoryRequire) sanitizeActualHistoryEventsForEquals(
	expectedHistoryEvents []*historypb.HistoryEvent,
	actualHistoryEvents []*historypb.HistoryEvent,
) []*historypb.HistoryEvent {

	if len(expectedHistoryEvents) != len(actualHistoryEvents) {
		panic("len(expectedHistoryEvents) != len(actualHistoryEvents)")
	}

	var sanitizedActualHistoryEvents []*historypb.HistoryEvent
	for i, actualHistoryEvent := range actualHistoryEvents {
		sanitizeEventID := expectedHistoryEvents[i].GetEventId() == 0
		sanitizeVersion := expectedHistoryEvents[i].GetVersion() == 0

		if !sanitizeEventID && !sanitizeVersion {
			sanitizedActualHistoryEvents = append(sanitizedActualHistoryEvents, actualHistoryEvent)
			continue
		}

		sanitizedActualHistoryEvent := proto.Clone(actualHistoryEvent).(*historypb.HistoryEvent)
		if sanitizeEventID {
			sanitizedActualHistoryEvent.EventId = 0
		}
		if sanitizeVersion {
			sanitizedActualHistoryEvent.Version = 0
		}
		sanitizedActualHistoryEvents = append(sanitizedActualHistoryEvents, sanitizedActualHistoryEvent)
	}
	return sanitizedActualHistoryEvents
}

// sanitizeActualHistoryEventsForContains clears EventID and Version fields from actual history events
// if they are not set in the FIRST expected history event.
func (h HistoryRequire) sanitizeActualHistoryEventsForContains(
	expectedHistoryEvents []*historypb.HistoryEvent,
	actualHistoryEvents []*historypb.HistoryEvent,
) []*historypb.HistoryEvent {
	if len(expectedHistoryEvents) == 0 {
		return actualHistoryEvents
	}

	sanitizeEventID := expectedHistoryEvents[0].GetEventId() == 0
	sanitizeVersion := expectedHistoryEvents[0].GetVersion() == 0

	if !sanitizeEventID && !sanitizeVersion {
		return actualHistoryEvents
	}

	var sanitizedActualHistoryEvents []*historypb.HistoryEvent
	for _, actualHistoryEvent := range actualHistoryEvents {
		sanitizedActualHistoryEvent := proto.Clone(actualHistoryEvent).(*historypb.HistoryEvent)
		if sanitizeEventID {
			sanitizedActualHistoryEvent.EventId = 0
		}
		if sanitizeVersion {
			sanitizedActualHistoryEvent.Version = 0
		}
		sanitizedActualHistoryEvents = append(sanitizedActualHistoryEvents, sanitizedActualHistoryEvent)
	}
	return sanitizedActualHistoryEvents
}

func (h HistoryRequire) equalHistoryEventsAttributes(
	expectedEventsAttributes []map[string]any,
	actualHistoryEvents []*historypb.HistoryEvent,
) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	for i, expectedEventAttributes := range expectedEventsAttributes {
		if expectedEventAttributes == nil {
			continue
		}
		actualHistoryEvent := actualHistoryEvents[i]
		actualEventAttributes := reflect.ValueOf(actualHistoryEvent.Attributes).Elem().Field(0).Elem()
		h.equalExpectedMapToActualAttributes(expectedEventAttributes, actualEventAttributes, actualHistoryEvent.EventId, "")
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
			versionStr = fmt.Sprintf("%3s ", "v"+strconv.Itoa(int(event.GetVersion())))
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

		if expectedValue == nil {
			if !actualV.IsNil() {
				require.Failf(h.t, "", "Value of property %s.%s for EventID=%v expected to be nil", attrPrefix, attrName, eventID)
			}
			continue
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
//
//nolint:revive // cognitive complexity 29 (> max enabled 25)
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
