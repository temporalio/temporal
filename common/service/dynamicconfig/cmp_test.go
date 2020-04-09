package dynamicconfig

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type compareEqualsTestSuite struct {
	suite.Suite
}

func TestCompareEqualsSuite(t *testing.T) {
	suite.Run(t, new(compareEqualsTestSuite))
}

func (s *compareEqualsTestSuite) TestIntCompareEquals() {
	s.True(intCompareEquals(0, 0))
	s.True(intCompareEquals(math.MaxInt32, math.MaxInt32))
	s.True(intCompareEquals(math.MinInt32, math.MinInt32))
	s.False(intCompareEquals(0, math.MaxInt32))
	s.False(intCompareEquals(math.MaxInt32, math.MinInt32))
}

func (s *compareEqualsTestSuite) TestFloat64CompareEquals() {
	s.True(float64CompareEquals(0.0, 0.0))
	s.True(float64CompareEquals(0.123456, 0.123456))
	s.True(float64CompareEquals(12345.123456, 12345.123456))
	s.False(float64CompareEquals(0.0, 0.1))
	s.False(float64CompareEquals(0.123456, 0.1234561))
	s.False(float64CompareEquals(12345.123456, 12345.1234567))
}

func (s *compareEqualsTestSuite) TestDurationCompareEquals() {
	s.True(durationCompareEquals(time.Second, time.Second))
	s.True(durationCompareEquals(time.Microsecond, time.Microsecond))
	s.True(durationCompareEquals(time.Millisecond, time.Millisecond))
	s.True(durationCompareEquals(time.Nanosecond, time.Nanosecond))
	s.True(durationCompareEquals(3600*time.Second, 3600*time.Second))
	s.False(durationCompareEquals(time.Nanosecond, time.Microsecond))
	s.False(durationCompareEquals(time.Microsecond+1, time.Microsecond))
}

func (s *compareEqualsTestSuite) TestMapCompareEquals() {
	buildMap := func() map[int]int {
		m := make(map[int]int)
		for i := 0; i < 10; i++ {
			m[rand.Int()] = rand.Int()
		}
		return m
	}

	cloneMap := func(src map[int]int) map[int]int {
		dst := make(map[int]int)
		for k, v := range src {
			dst[k] = v
		}
		return dst
	}

	for i := 0; i < 10; i++ {
		m := buildMap()
		s.True(reflect.DeepEqual(m, cloneMap(m)))
	}
}
