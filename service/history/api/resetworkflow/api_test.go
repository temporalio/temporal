package resetworkflow

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/enums/v1"
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
	// Include all with no exclusions is no exclusions
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{},
			enums.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
		[]enums.ResetReapplyExcludeType{},
	)
	// Include all with one exclusion is one exclusion
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enums.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
		[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
	)
	// Include signal with no exclusions is no exclusions
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{},
			enums.RESET_REAPPLY_TYPE_SIGNAL,
		),
		[]enums.ResetReapplyExcludeType{},
	)
	// Include signal with exclude signal: exclude trumps deprecated include
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enums.RESET_REAPPLY_TYPE_SIGNAL,
		),
		[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
	)
	// Include none with no exclusions is all excluded
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{},
			enums.RESET_REAPPLY_TYPE_NONE,
		),
		[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
	)
	// Include none with exclude signal is all excluded
	s.Equal(
		GetResetReapplyExcludeTypes(
			[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enums.RESET_REAPPLY_TYPE_NONE,
		),
		[]enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
	)
}
