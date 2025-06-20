package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	VersionTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestVersionTestSuite(t *testing.T) {
	suite.Run(t, new(VersionTestSuite))
}

func (s *VersionTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *VersionTestSuite) TestNormalizeVersionStringSuccess() {

	validInputs := []string{"0", "1000", "9999", "0.1", "0.9", "99.9", "100.8"}
	for _, in := range validInputs {
		ans, err := normalizeVersionString(in)
		s.NoError(err, in)
		s.Equal(in, ans, in)

		withPrefix := "v" + in
		ans, err = normalizeVersionString(withPrefix)
		s.NoError(err, in)
		s.Equal(in, ans, in)
	}

}

func (s *VersionTestSuite) TestNormalizeVersionStringInvalidInput() {

	errInputs := []string{"1.2a", "ab", "5.11a"}
	for _, in := range errInputs {
		_, err := normalizeVersionString(in)
		s.Error(err, in)
		_, err = normalizeVersionString("v" + in)
		s.Errorf(err, in)
	}
}
