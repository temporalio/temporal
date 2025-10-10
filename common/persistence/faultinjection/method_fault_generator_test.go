package faultinjection

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	methodFaultGeneratorSuite struct {
		suite.Suite
	}
)

func TestMethodFaultGeneratorSuite(t *testing.T) {
	s := new(methodFaultGeneratorSuite)
	suite.Run(t, s)
}

func (s *methodFaultGeneratorSuite) SetupSuite() {}

func (s *methodFaultGeneratorSuite) TearDownSuite() {}



func (s *methodFaultGeneratorSuite) TearDownTest() {}

func (s *methodFaultGeneratorSuite) Test_Generate() {
	faults := []fault{
		{
			err:    errors.New("random error0"),
			execOp: false,
			rate:   0.01,
		},
		{
			err:    errors.New("random error1"),
			execOp: false,
			rate:   0.11,
		},
		{
			err:    errors.New("random error2"),
			execOp: false,
			rate:   0.22,
		},
	}
	gen := newMethodFaultGenerator(faults, 2208)

	require.EqualValues(s.T(), 34, math.Round(gen.rate*100))
	require.Len(s.T(), gen.faultsMetadata, 3)
	require.EqualValues(s.T(), 1, math.Round(gen.faultsMetadata[0].threshold*100))
	require.EqualValues(s.T(), 12, math.Round(gen.faultsMetadata[1].threshold*100))
	require.EqualValues(s.T(), 34, math.Round(gen.faultsMetadata[2].threshold*100))

	f1 := gen.generate("")
	require.Nil(s.T(), f1)
	f2 := gen.generate("")
	require.NotNil(s.T(), f2)
	require.Equal(s.T(), faults[2], *f2)
	f3 := gen.generate("")
	require.NotNil(s.T(), f3)
	require.Equal(s.T(), faults[2], *f3)
	f4 := gen.generate("")
	require.Nil(s.T(), f4)
}
