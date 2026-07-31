package sqlquery

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	QueryTemplate = "select * from dummy where %s"

	DefaultDateTimeFormat = time.RFC3339
)

func ConvertToTime(timeStr string) (time.Time, error) {
	ts, err := strconv.ParseInt(timeStr, 10, 64)
	if err == nil {
		return timestamp.UnixOrZeroTime(ts), nil
	}
	timestampStr, err := ExtractStringValue(timeStr)
	if err != nil {
		return time.Time{}, err
	}
	parsedTime, err := time.Parse(DefaultDateTimeFormat, timestampStr)
	if err != nil {
		return time.Time{}, err
	}
	return parsedTime, nil
}

func ExtractStringValue(s string) (string, error) {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1], nil
	}
	return "", fmt.Errorf("value %s is not a string value", s)
}

// ConvertStatusStr converts a workflow execution status filter value to its enum
// representation. Both the status name and its numeric value are accepted.
func ConvertStatusStr(statusStr string) (enumspb.WorkflowExecutionStatus, error) {
	statusStr = strings.ToLower(strings.TrimSpace(statusStr))
	switch statusStr {
	case "completed", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, nil
	case "failed", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nil
	case "canceled", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, nil
	case "terminated", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, nil
	case "continuedasnew", "continued_as_new", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, nil
	case "timedout", "timed_out", convert.Int32ToString(int32(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT)):
		return enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, nil
	default:
		return 0, fmt.Errorf("unknown workflow close status: %s", statusStr)
	}
}

func ExtractIntValue(s string) (int, error) {
	intValue, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return intValue, nil
}

// ParseValue returns a string, int64 or float64 if the parsing succeeds.
func ParseValue(sqlValue string) (any, error) {
	if sqlValue == "" {
		return "", nil
	}

	if sqlValue[0] == '\'' && sqlValue[len(sqlValue)-1] == '\'' {
		strValue := strings.Trim(sqlValue, "'")
		return strValue, nil
	}

	// Unquoted value must be a number. Try int64 first.
	if intValue, err := strconv.ParseInt(sqlValue, 10, 64); err == nil {
		return intValue, nil
	}

	// Then float64.
	if floatValue, err := strconv.ParseFloat(sqlValue, 64); err == nil {
		return floatValue, nil
	}

	return nil, serviceerror.NewInvalidArgumentf("invalid expression: unable to parse %s", sqlValue)
}
