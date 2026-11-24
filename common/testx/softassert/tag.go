package softassert

// Field supplies a key and value for structured logging.
type Field interface {
	Key() string
	Value() any
}

type failedAssertionTag struct{}

func (failedAssertionTag) Key() string {
	return "failed-assertion"
}

func (failedAssertionTag) Value() any {
	return true
}

// FailedAssertion marks a log entry as a failed soft assertion.
var FailedAssertion = failedAssertionTag{}
