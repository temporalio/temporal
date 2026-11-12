package softassert

type failedAssertionTag struct{}

func (failedAssertionTag) Key() string { return "failed-assertion" }
func (failedAssertionTag) Value() any  { return true }

// FailedAssertion marks a log entry as a failed soft assertion.
var FailedAssertion = failedAssertionTag{}
