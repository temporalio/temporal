package sharding

import (
	"fmt"
	"os"
	"strconv"
)

// GenerateRepresentativeTestNames creates a sample of realistic test names for analysis
func GenerateRepresentativeTestNames(count int) []string {
	testNames := make([]string, count)

	prefixes := []string{
		"TestWorkflow", "TestActivity", "TestTimer", "TestSignal", "TestQuery",
		"TestChild", "TestContinue", "TestCancel", "TestRetry", "TestTimeout",
		"TestUpdate", "TestCron", "TestSchedule", "TestBatch", "TestLocal",
	}

	suffixes := []string{
		"Basic", "Advanced", "Error", "Success", "Timeout", "Cancel", "Retry",
		"Large", "Small", "Complex", "Simple", "Async", "Sync", "Batch",
		"Integration", "Unit", "Functional", "Performance", "Stress",
	}

	scenarios := []string{
		"WithSignal", "WithTimer", "WithChild", "WithQuery", "WithError",
		"WithRetry", "WithTimeout", "WithCancel", "WithUpdate", "WithSearch",
		"WithHistory", "WithArchival", "WithVisibility", "WithNamespace", "WithCluster",
	}

	for i := 0; i < count; i++ {
		prefix := prefixes[i%len(prefixes)]
		suffix := suffixes[(i/len(prefixes))%len(suffixes)]
		scenario := scenarios[(i/(len(prefixes)*len(suffixes)))%len(scenarios)]

		testNames[i] = fmt.Sprintf("%s%s%s_%d", prefix, suffix, scenario, i%100)
	}

	return testNames
}

// getEnvWithDefault returns environment variable value or default
func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvBool returns environment variable as boolean
func getEnvBool(key string) bool {
	return os.Getenv(key) == "true"
}

// getEnvInt returns environment variable as int with default
func getEnvInt(key string, defaultValue int) int {
	if str := os.Getenv(key); str != "" {
		if val, err := strconv.Atoi(str); err == nil {
			return val
		}
	}
	return defaultValue
}