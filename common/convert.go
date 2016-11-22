package common

// IntPtr makes a copy and returns the pointer to an int.
func IntPtr(v int) *int {
	return &v
}

// Int32Ptr makes a copy and returns the pointer to an int32.
func Int32Ptr(v int32) *int32 {
	return &v
}

// Int64Ptr makes a copy and returns the pointer to an int64.
func Int64Ptr(v int64) *int64 {
	return &v
}

// Uint32Ptr makes a copy and returns the pointer to a uint32.
func Uint32Ptr(v uint32) *uint32 {
	return &v
}

// Uint64Ptr makes a copy and returns the pointer to a uint64.
func Uint64Ptr(v uint64) *uint64 {
	return &v
}

// Float64Ptr makes a copy and returns the pointer to an int64.
func Float64Ptr(v float64) *float64 {
	return &v
}

// BoolPtr makes a copy and returns the pointer to a bool.
func BoolPtr(v bool) *bool {
	return &v
}

// StringPtr makes a copy and returns the pointer to a string.
func StringPtr(v string) *string {
	return &v
}
