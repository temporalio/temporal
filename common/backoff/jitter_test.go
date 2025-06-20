package backoff

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type (
	jitterSuite struct {
		suite.Suite
	}
)

func TestJitterSuite(t *testing.T) {
	s := new(jitterSuite)
	suite.Run(t, s)
}

func (s *jitterSuite) SetupSuite() {
}

func (s *jitterSuite) TestJitter_Int64() {
	input := int64(1048576)
	coefficient := float64(0.25)
	lowerBound := int64(float64(input) * (1 - coefficient))
	upperBound := int64(float64(input) * (1 + coefficient))

	for i := 0; i < 1048576; i++ {
		result := Jitter(input, coefficient)
		s.True(result >= lowerBound)
		s.True(result < upperBound)

		result = FullJitter(input)
		s.True(result >= 0)
		s.True(result < input)
	}
}

func (s *jitterSuite) TestJitter_Float64() {
	input := float64(1048576.1048576)
	coefficient := float64(0.16)
	lowerBound := float64(input) * (1 - coefficient)
	upperBound := float64(input) * (1 + coefficient)

	for i := 0; i < 1048576; i++ {
		result := Jitter(input, coefficient)
		s.True(result >= lowerBound)
		s.True(result < upperBound)

		result = FullJitter(input)
		s.True(result >= 0)
		s.True(result < input)
	}
}

func (s *jitterSuite) TestJitter_Duration() {
	input := time.Duration(1099511627776)
	coefficient := float64(0.1)
	lowerBound := time.Duration(int64(float64(input.Nanoseconds()) * (1 - coefficient)))
	upperBound := time.Duration(int64(float64(input.Nanoseconds()) * (1 + coefficient)))

	for i := 0; i < 1048576; i++ {
		result := Jitter(input, coefficient)
		s.True(result >= lowerBound)
		s.True(result < upperBound)

		result = FullJitter(input)
		s.True(result >= 0)
		s.True(result < input)
	}
}

func (s *jitterSuite) TestJitter_InputZeroValue() {
	s.Zero(Jitter(time.Duration(0), rand.Float64()))
	s.Zero(Jitter(int64(0), rand.Float64()))
	s.Zero(Jitter(float64(0), rand.Float64()))
}

func (s *jitterSuite) TestJitter_CoeffientZeroValue() {
	s.Equal(time.Duration(1), Jitter(time.Duration(1), 0))
	s.Equal(int64(1), Jitter(int64(1), 0))
	s.Equal(float64(1), Jitter(float64(1), 0))
}
