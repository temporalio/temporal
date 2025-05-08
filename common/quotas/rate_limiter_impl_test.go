package quotas

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	rateLimiterSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestRateLimiterSuite(t *testing.T) {
	s := new(rateLimiterSuite)
	suite.Run(t, s)
}

func (s *rateLimiterSuite) SetupSuite() {

}

func (s *rateLimiterSuite) TearDownSuite() {

}

func (s *rateLimiterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *rateLimiterSuite) TearDownTest() {

}

func (s *rateLimiterSuite) TestSetRate_Same() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	rateLimiterBefore := rateLimiter.ClockedRateLimiter
	rateLimiter.SetRPS(testRate)
	rateLimiterAfter := rateLimiter.ClockedRateLimiter
	s.Equal(testRate, rateLimiter.Rate())
	s.Equal(testBurst, rateLimiter.Burst())
	s.Equal(rateLimiterBefore, rateLimiterAfter)
}

func (s *rateLimiterSuite) TestSetRate_Diff() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	newRate := testRate * 2
	rateLimiter.SetRPS(newRate)
	s.Equal(newRate, rateLimiter.Rate())
	s.Equal(testBurst, rateLimiter.Burst())
}

func (s *rateLimiterSuite) TestSetBurst_Same() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	rateLimiterBefore := rateLimiter.ClockedRateLimiter
	rateLimiter.SetBurst(testBurst)
	rateLimiterAfter := rateLimiter.ClockedRateLimiter
	s.Equal(testRate, rateLimiter.Rate())
	s.Equal(testBurst, rateLimiter.Burst())
	s.Equal(rateLimiterBefore, rateLimiterAfter)
}

func (s *rateLimiterSuite) TestSetBurst_Diff() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	newBurst := testBurst * 2
	rateLimiter.SetBurst(newBurst)
	s.Equal(testRate, rateLimiter.Rate())
	s.Equal(newBurst, rateLimiter.Burst())
}

func (s *rateLimiterSuite) TestSetRateBurst_Same() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	rateLimiterBefore := rateLimiter.ClockedRateLimiter
	rateLimiter.SetRateBurst(rateLimiter.Rate(), rateLimiter.Burst())
	rateLimiterAfter := rateLimiter.ClockedRateLimiter
	s.Equal(testRate, rateLimiter.Rate())
	s.Equal(testBurst, rateLimiter.Burst())
	s.Equal(rateLimiterBefore, rateLimiterAfter)
}

func (s *rateLimiterSuite) TestSetRateBurst_Diff() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	newRate := rateLimiter.Rate() * 2
	newBurst := rateLimiter.Burst()
	rateLimiter.SetRateBurst(newRate, newBurst)
	s.Equal(newRate, rateLimiter.Rate())
	s.Equal(newBurst, rateLimiter.Burst())

	newRate = rateLimiter.Rate()
	newBurst = rateLimiter.Burst() * 2
	rateLimiter.SetRateBurst(newRate, newBurst)
	s.Equal(newRate, rateLimiter.Rate())
	s.Equal(newBurst, rateLimiter.Burst())

	newRate = rateLimiter.Rate() * 2
	newBurst = rateLimiter.Burst() * 2
	rateLimiter.SetRateBurst(newRate, newBurst)
	s.Equal(newRate, rateLimiter.Rate())
	s.Equal(newBurst, rateLimiter.Burst())
}
