package backoff

import (
	"log"
	"os"
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
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *jitterSuite) TestJitInt64() {
	input := int64(1048576)
	coefficient := float64(0.25)
	lowerBound := int64(float64(input) * (1 - coefficient))
	upperBound := int64(float64(input) * (1 + coefficient))

	for i := 0; i < 1048576; i++ {
		result := JitInt64(input, coefficient)
		s.True(result >= lowerBound)
		s.True(result < upperBound)
	}
}

func (s *jitterSuite) TestJitFloat64() {
	input := float64(1048576.1048576)
	coefficient := float64(0.16)
	lowerBound := float64(input) * (1 - coefficient)
	upperBound := float64(input) * (1 + coefficient)

	for i := 0; i < 1048576; i++ {
		result := JitFloat64(input, coefficient)
		s.True(result >= lowerBound)
		s.True(result < upperBound)
	}
}

func (s *jitterSuite) TestJitDuration() {
	input := time.Duration(1099511627776)
	coefficient := float64(0.1)
	lowerBound := time.Duration(int64(float64(input.Nanoseconds()) * (1 - coefficient)))
	upperBound := time.Duration(int64(float64(input.Nanoseconds()) * (1 + coefficient)))

	for i := 0; i < 1048576; i++ {
		result := JitDuration(input, coefficient)
		s.True(result >= lowerBound)
		s.True(result < upperBound)
	}
}
