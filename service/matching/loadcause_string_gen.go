// Code generated by "stringer -type loadCause -trimprefix loadCause -output loadcause_string_gen.go"; DO NOT EDIT.

package matching

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[loadCauseUnspecified-0]
	_ = x[loadCauseTask-1]
	_ = x[loadCauseQuery-2]
	_ = x[loadCauseDescribe-3]
	_ = x[loadCauseUserData-4]
	_ = x[loadCauseNexusTask-5]
	_ = x[loadCausePoll-6]
	_ = x[loadCauseOtherRead-7]
	_ = x[loadCauseOtherWrite-8]
	_ = x[loadCauseForce-9]
}

const _loadCause_name = "UnspecifiedTaskQueryDescribeUserDataNexusTaskPollOtherReadOtherWriteForce"

var _loadCause_index = [...]uint8{0, 11, 15, 20, 28, 36, 45, 49, 58, 68, 73}

func (i loadCause) String() string {
	if i < 0 || i >= loadCause(len(_loadCause_index)-1) {
		return "loadCause(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _loadCause_name[_loadCause_index[i]:_loadCause_index[i+1]]
}
