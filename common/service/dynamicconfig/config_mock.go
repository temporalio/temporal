package dynamicconfig

import "time"

// These mock functions are for tests to use config properties that are dynamic

// GetIntPropertyFn returns value as IntPropertyFn
func GetIntPropertyFn(value int) func(opts ...FilterOption) int {
	return func(...FilterOption) int { return value }
}

// GetIntPropertyFilteredByNamespace returns values as IntPropertyFnWithNamespaceFilters
func GetIntPropertyFilteredByNamespace(value int) func(namespace string) int {
	return func(namespace string) int { return value }
}

// GetIntPropertyFilteredByTaskListInfo returns value as IntPropertyFnWithTaskListInfoFilters
func GetIntPropertyFilteredByTaskListInfo(value int) func(namespace string, taskList string, taskType int32) int {
	return func(namespace string, taskList string, taskType int32) int { return value }
}

// GetFloatPropertyFn returns value as FloatPropertyFn
func GetFloatPropertyFn(value float64) func(opts ...FilterOption) float64 {
	return func(...FilterOption) float64 { return value }
}

// GetBoolPropertyFn returns value as BoolPropertyFn
func GetBoolPropertyFn(value bool) func(opts ...FilterOption) bool {
	return func(...FilterOption) bool { return value }
}

// GetBoolPropertyFnFilteredByNamespace returns value as BoolPropertyFnWithNamespaceFilters
func GetBoolPropertyFnFilteredByNamespace(value bool) func(namespace string) bool {
	return func(namespace string) bool { return value }
}

// GetDurationPropertyFnFilteredByNamespace returns value as DurationPropertyFnFilteredByNamespace
func GetDurationPropertyFnFilteredByNamespace(value time.Duration) func(namespace string) time.Duration {
	return func(namespace string) time.Duration { return value }
}

// GetDurationPropertyFn returns value as DurationPropertyFn
func GetDurationPropertyFn(value time.Duration) func(opts ...FilterOption) time.Duration {
	return func(...FilterOption) time.Duration { return value }
}

// GetDurationPropertyFnFilteredByTaskListInfo returns value as DurationPropertyFnWithTaskListInfoFilters
func GetDurationPropertyFnFilteredByTaskListInfo(value time.Duration) func(namespace string, taskList string, taskType int32) time.Duration {
	return func(namespace string, taskList string, taskType int32) time.Duration { return value }
}

// GetStringPropertyFn returns value as StringPropertyFn
func GetStringPropertyFn(value string) func(opts ...FilterOption) string {
	return func(...FilterOption) string { return value }
}

// GetMapPropertyFn returns value as MapPropertyFn
func GetMapPropertyFn(value map[string]interface{}) func(opts ...FilterOption) map[string]interface{} {
	return func(...FilterOption) map[string]interface{} { return value }
}
