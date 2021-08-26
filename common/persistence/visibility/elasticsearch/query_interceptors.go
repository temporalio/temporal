package elasticsearch

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/elasticsearch/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	nameInterceptor struct {
		index                    string
		searchAttributesProvider searchattribute.Provider
	}
	valuesInterceptor struct{}
)

func newNameInterceptor(
	index string,
	searchAttributesProvider searchattribute.Provider,
) *nameInterceptor {
	return &nameInterceptor{
		index:                    index,
		searchAttributesProvider: searchAttributesProvider,
	}
}

func newValuesInterceptor() *valuesInterceptor {
	return &valuesInterceptor{}
}

func (ni *nameInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	if usage == query.FieldNameSorter {
		searchAttributes, err := ni.searchAttributesProvider.GetSearchAttributes(ni.index, false)
		if err != nil {
			return "", fmt.Errorf("unable to read search attribute types: %v", err)
		}
		fieldType, _ := searchAttributes.GetType(name)
		if fieldType == enumspb.INDEXED_VALUE_TYPE_STRING {
			return "", errors.New("unable to sort by field of String type, use field of type Keyword")
		}
	}
	return name, nil
}

func (vi *valuesInterceptor) Values(name string, values ...interface{}) ([]interface{}, error) {
	var result []interface{}
	for _, value := range values {

		switch name {
		case searchattribute.StartTime, searchattribute.CloseTime, searchattribute.ExecutionTime:
			if nanos, isNumber := value.(int64); isNumber {
				value = time.Unix(0, int64(nanos)).UTC().Format(time.RFC3339Nano)
			}
		case searchattribute.ExecutionStatus:
			if status, isNumber := value.(int64); isNumber {
				value = enumspb.WorkflowExecutionStatus_name[int32(status)]
			}
		case searchattribute.ExecutionDuration:
			if durationStr, isString := value.(string); isString {
				// To support durations passed as golang durations such as "300ms", "-1.5h" or "2h45m".
				// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
				if duration, err := time.ParseDuration(durationStr); err == nil {
					value = duration.Nanoseconds()
				} else {
					// To support "hh:mm:ss" durations.
					durationNanos, err := vi.parseHHMMSSDuration(durationStr)
					if errors.Is(err, ErrInvalidDuration) {
						return nil, err
					}
					if err == nil {
						value = durationNanos
					}
				}
			}
		default:
		}

		result = append(result, value)
	}
	return result, nil
}

func (vi *valuesInterceptor) parseHHMMSSDuration(d string) (int64, error) {
	var hours, minutes, seconds, nanos int64
	_, err := fmt.Sscanf(d, "%d:%d:%d", &hours, &minutes, &seconds)
	if err != nil {
		return 0, err
	}
	if hours < 0 {
		return 0, fmt.Errorf("%w: hours must be positive number", ErrInvalidDuration)
	}
	if minutes < 0 || minutes > 59 {
		return 0, fmt.Errorf("%w: minutes must be from 0 to 59", ErrInvalidDuration)
	}
	if seconds < 0 || seconds > 59 {
		return 0, fmt.Errorf("%w: seconds must be from 0 to 59", ErrInvalidDuration)
	}

	return hours*int64(time.Hour) + minutes*int64(time.Minute) + seconds*int64(time.Second) + nanos, nil
}
