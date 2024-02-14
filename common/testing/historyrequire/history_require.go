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

func New(t require.TestingT) HistoryRequire {
	return HistoryRequire{
		t: t,
	}
}

func (h HistoryRequire) EqualHistoryEvents(expectedHistory string, actualHistoryEvents []*historypb.HistoryEvent) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	h.EqualHistory(expectedHistory, &historypb.History{Events: actualHistoryEvents})
}

func (h HistoryRequire) EqualHistory(expectedHistory string, actualHistory *historypb.History) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	expectedCompactHistory, expectedEventsAttributes := h.parseHistory(expectedHistory)
	actualCompactHistory := h.formatHistoryCompact(actualHistory)
	require.Equal(h.t, expectedCompactHistory, actualCompactHistory)
	for _, actualHistoryEvent := range actualHistory.Events {
		if expectedEventAttributes, ok := expectedEventsAttributes[actualHistoryEvent.EventId]; ok {
			actualEventAttributes := reflect.ValueOf(actualHistoryEvent.Attributes).Elem().Field(0).Elem()
			h.equalStructToMap(expectedEventAttributes, actualEventAttributes, actualHistoryEvent.EventId, "")
		}
	}
}

func (h HistoryRequire) PrintHistory(history *historypb.History) {
	_, _ = fmt.Println(h.formatHistory(history))
}

func (h HistoryRequire) PrintHistoryEvents(events []*historypb.HistoryEvent) {
	h.PrintHistory(&historypb.History{Events: events})
}

func (h HistoryRequire) PrintHistoryCompact(history *historypb.History) {
	_, _ = fmt.Println(h.formatHistoryCompact(history))
}

func (h HistoryRequire) PrintHistoryEventsCompact(events []*historypb.HistoryEvent) {
	h.PrintHistoryCompact(&historypb.History{Events: events})
}

func (h HistoryRequire) formatHistoryCompact(history *historypb.History) string {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	var sb strings.Builder
	for _, event := range history.Events {
		_, _ = sb.WriteString(fmt.Sprintf("%3d %s\n", event.GetEventId(), event.GetEventType()))
	}
	if sb.Len() > 0 {
		return sb.String()[:sb.Len()-1]
	}
	return ""
}

func (h HistoryRequire) formatHistory(history *historypb.History) string {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	var sb strings.Builder
	for _, event := range history.Events {
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

		var fieldData any
		if field.Kind() == reflect.Pointer && field.IsNil() {
			continue
		} else if field.Kind() == reflect.Pointer && field.Elem().Kind() == reflect.Struct {
			fieldData = h.structToMap(field.Elem().Interface())
		} else if field.Kind() == reflect.Struct {
			fieldData = h.structToMap(field.Interface())
		} else {
			fieldData = field.Interface()
		}
		ret[strctT.Field(i).Name] = fieldData
	}

	return ret
}

func (h HistoryRequire) equalStructToMap(expectedMap map[string]any, actualStructV reflect.Value, eventID int64, attrPrefix string) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	for attrName, expectedValue := range expectedMap {
		actualFieldV := actualStructV.FieldByName(attrName)
		if actualFieldV.Kind() == reflect.Invalid {
			require.Failf(h.t, "", "Expected property %s%s wasn't found for EventID=%v", attrPrefix, attrName, eventID)
		}

		if ep, ok := expectedValue.(map[string]any); ok {
			if actualFieldV.IsNil() {
				require.Failf(h.t, "", "Value of property %s%s for EventID=%v expected to be struct but was nil", attrPrefix, attrName, eventID)
			}
			if actualFieldV.Kind() == reflect.Pointer {
				actualFieldV = actualFieldV.Elem()
			}
			if actualFieldV.Kind() != reflect.Struct {
				require.Failf(h.t, "", "Value of property %s%s for EventID=%v expected to be struct but was of type %s", attrPrefix, attrName, eventID, actualFieldV.Type().String())
			}
			h.equalStructToMap(ep, actualFieldV, eventID, attrPrefix+attrName+".")
			continue
		}
		actualFieldValue := actualFieldV.Interface()
		require.EqualValues(h.t, expectedValue, actualFieldValue, "Values of %s%s property are not equal for EventID=%v", attrPrefix, attrName, eventID)
	}
}

// parseHistory accept history in a formatHistory format and returns compact history string and map of eventID to map of event attributes.
func (h HistoryRequire) parseHistory(expectedHistory string) (string, map[int64]map[string]any) {
	if th, ok := h.t.(helper); ok {
		th.Helper()
	}

	hist := &historypb.History{}
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
		hist.Events = append(hist.Events, &historypb.HistoryEvent{
			EventId:   int64(eventID),
			EventType: eventType,
		})
		var jb strings.Builder
		for i := 2; i < len(fields); i++ {
			if strings.HasPrefix(fields[i], "//") {
				break
			}
			_, _ = jb.WriteString(fields[i])
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
	return h.formatHistoryCompact(hist), eventsAttrs
}
