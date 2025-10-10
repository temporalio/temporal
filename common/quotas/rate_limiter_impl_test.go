package quotas

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	rateLimiterSuite struct {
		suite.Suite
	}
)

func TestRateLimiterSuite(t *testing.T) {
	s := new(rateLimiterSuite)
	suite.Run(t, s)
}



func (s *rateLimiterSuite) TearDownSuite() {

}



func (s *rateLimiterSuite) TearDownTest() {

}

func (s *rateLimiterSuite) TestSetRate_Same() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	rateLimiterBefore := rateLimiter.ClockedRateLimiter
	rateLimiter.SetRPS(testRate)
	rateLimiterAfter := rateLimiter.ClockedRateLimiter
	require.Equal(s.T(), testRate, rateLimiter.Rate())
	require.Equal(s.T(), testBurst, rateLimiter.Burst())
	require.Equal(s.T(), rateLimiterBefore, rateLimiterAfter)
}

func (s *rateLimiterSuite) TestSetRate_Diff() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	newRate := testRate * 2
	rateLimiter.SetRPS(newRate)
	require.Equal(s.T(), newRate, rateLimiter.Rate())
	require.Equal(s.T(), testBurst, rateLimiter.Burst())
}

func (s *rateLimiterSuite) TestSetBurst_Same() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	rateLimiterBefore := rateLimiter.ClockedRateLimiter
	rateLimiter.SetBurst(testBurst)
	rateLimiterAfter := rateLimiter.ClockedRateLimiter
	require.Equal(s.T(), testRate, rateLimiter.Rate())
	require.Equal(s.T(), testBurst, rateLimiter.Burst())
	require.Equal(s.T(), rateLimiterBefore, rateLimiterAfter)
}

func (s *rateLimiterSuite) TestSetBurst_Diff() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	newBurst := testBurst * 2
	rateLimiter.SetBurst(newBurst)
	require.Equal(s.T(), testRate, rateLimiter.Rate())
	require.Equal(s.T(), newBurst, rateLimiter.Burst())
}

func (s *rateLimiterSuite) TestSetRateBurst_Same() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	rateLimiterBefore := rateLimiter.ClockedRateLimiter
	rateLimiter.SetRateBurst(rateLimiter.Rate(), rateLimiter.Burst())
	rateLimiterAfter := rateLimiter.ClockedRateLimiter
	require.Equal(s.T(), testRate, rateLimiter.Rate())
	require.Equal(s.T(), testBurst, rateLimiter.Burst())
	require.Equal(s.T(), rateLimiterBefore, rateLimiterAfter)
}

func (s *rateLimiterSuite) TestSetRateBurst_Diff() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	newRate := rateLimiter.Rate() * 2
	newBurst := rateLimiter.Burst()
	rateLimiter.SetRateBurst(newRate, newBurst)
	require.Equal(s.T(), newRate, rateLimiter.Rate())
	require.Equal(s.T(), newBurst, rateLimiter.Burst())

	newRate = rateLimiter.Rate()
	newBurst = rateLimiter.Burst() * 2
	rateLimiter.SetRateBurst(newRate, newBurst)
	require.Equal(s.T(), newRate, rateLimiter.Rate())
	require.Equal(s.T(), newBurst, rateLimiter.Burst())

	newRate = rateLimiter.Rate() * 2
	newBurst = rateLimiter.Burst() * 2
	rateLimiter.SetRateBurst(newRate, newBurst)
	require.Equal(s.T(), newRate, rateLimiter.Rate())
	require.Equal(s.T(), newBurst, rateLimiter.Burst())
}
