// Code generated by "stringer -type unloadCause -trimprefix unloadCause -output unloadcause_string_gen.go"; DO NOT EDIT.

package matching

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[unloadCauseUnspecified-0]
	_ = x[unloadCauseInitError-1]
	_ = x[unloadCauseIdle-2]
	_ = x[unloadCauseMembership-3]
	_ = x[unloadCauseConflict-4]
	_ = x[unloadCauseShuttingDown-5]
	_ = x[unloadCauseForce-6]
	_ = x[unloadCauseConfigChange-7]
	_ = x[unloadCauseOtherError-8]
}

const _unloadCause_name = "UnspecifiedInitErrorIdleMembershipConflictShuttingDownForceConfigChangeOtherError"

var _unloadCause_index = [...]uint8{0, 11, 20, 24, 34, 42, 54, 59, 71, 81}

func (i unloadCause) String() string {
	if i < 0 || i >= unloadCause(len(_unloadCause_index)-1) {
		return "unloadCause(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _unloadCause_name[_unloadCause_index[i]:_unloadCause_index[i+1]]
}
