package resetworkflow

import (
	"testing"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
)

type (
	resetWorkflowSuite struct {
		suite.Suite
	}
)

func TestResetWorkflowSuite(t *testing.T) {
	s := new(resetWorkflowSuite)
	suite.Run(t, s)
}

func (s *resetWorkflowSuite) TestGetResetReapplyExcludeTypes() {
	// Include all with no exclusions => no exclusions
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{},
			enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
	)
	// Include all with one exclusion => one exclusion (honor exclude in presence of default value of deprecated option)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {}},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
	)
	// Include signal with no exclusions => exclude updates
	// (honor non-default value of deprecated option in presence of default value of non-deprecated option)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {}},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{},
			enumspb.RESET_REAPPLY_TYPE_SIGNAL,
		),
	)
	// Include signal with exclude signal => include signal means they want to exclude updates, and then the explicit
	// exclusion of signal trumps the deprecated inclusion
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enumspb.RESET_REAPPLY_TYPE_SIGNAL,
		),
	)
	// Include none with no exclusions => all excluded
	// (honor non-default value of deprecated option in presence of default value of non-deprecated option)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{},
			enumspb.RESET_REAPPLY_TYPE_NONE,
		),
	)
	// Include none with exclude signal is all excluded
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enumspb.RESET_REAPPLY_TYPE_NONE,
		),
	)
}
