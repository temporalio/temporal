package testrunner

import "go.temporal.io/server/tools/common/gotestparse"

// The go-test output parsing lives in the shared gotestparse package (also used by
// flakeshake). These aliases re-export it under the names used throughout testrunner.
type (
	failureType = gotestparse.FailureType
	alert       = gotestparse.Alert
)

const (
	failureTypeFailed   = gotestparse.FailureTypeFailed
	failureTypeTimeout  = gotestparse.FailureTypeTimeout
	failureTypeCrash    = gotestparse.FailureTypeCrash
	failureTypeDataRace = gotestparse.FailureTypeDataRace
	failureTypePanic    = gotestparse.FailureTypePanic
	failureTypeFatal    = gotestparse.FailureTypeFatal
	noFailureDetails    = gotestparse.NoFailureDetails
)

var (
	parseAlerts                = gotestparse.ParseAlerts
	parseFailedTestsFromOutput = gotestparse.ParseFailedTestsFromOutput
	parseTestTimeouts          = gotestparse.ParseTestTimeouts
	parseFailureDetails        = gotestparse.ParseFailureDetails
	primaryTestName            = gotestparse.PrimaryTestName
)
