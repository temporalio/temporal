package tasks

import (
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	taskKeySuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestTaskKeySuite(t *testing.T) {
	s := new(taskKeySuite)
	suite.Run(t, s)
}

func (s *taskKeySuite) SetupSuite() {

}

func (s *taskKeySuite) TearDownSuite() {

}

func (s *taskKeySuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *taskKeySuite) TearDownTest() {

}

func (s *taskKeySuite) TestValidateKey_Valid() {
	s.NoError(ValidateKey(Key{
		FireTime: time.Unix(0, 0),
		TaskID:   0,
	}))
	s.NoError(ValidateKey(Key{
		FireTime: time.Unix(0, math.MaxInt64),
		TaskID:   math.MaxInt64,
	}))
	s.NoError(ValidateKey(Key{
		FireTime: time.Unix(0, rand.Int63()),
		TaskID:   rand.Int63(),
	}))
}

func (s *taskKeySuite) TestValidateKey_Invalid() {
	s.Error(ValidateKey(Key{
		FireTime: time.Time{},
		TaskID:   0,
	}))
	s.Error(ValidateKey(Key{
		FireTime: time.Now(),
		TaskID:   -1,
	}))
}

func (s *taskKeySuite) TestMinMaxKey() {
	thisKey := NewKey(time.Unix(0, rand.Int63()), rand.Int63())
	thatKey := NewKey(time.Unix(0, rand.Int63()), rand.Int63())

	minKey := MinKey(thisKey, thatKey)
	s.True(minKey.CompareTo(thisKey) <= 0)
	s.True(minKey.CompareTo(thatKey) <= 0)

	maxKey := MaxKey(thisKey, thatKey)
	s.True(maxKey.CompareTo(thisKey) >= 0)
	s.True(maxKey.CompareTo(thisKey) >= 0)
}

func (s *taskKeySuite) TestSort() {
	numInstant := 256
	numTaskPerInstant := 16

	taskKeys := Keys{}
	for i := 0; i < numInstant; i++ {
		fireTime := time.Unix(0, rand.Int63())
		for j := 0; j < numTaskPerInstant; j++ {
			taskKeys = append(taskKeys, NewKey(fireTime, rand.Int63()))
		}
	}
	sort.Sort(taskKeys)

	for i := 1; i < numInstant*numTaskPerInstant; i++ {
		prev := taskKeys[i-1]
		next := taskKeys[i]

		if prev.FireTime.Before(next.FireTime) {
			// noop
		} else if prev.FireTime.Equal(next.FireTime) {
			s.True(prev.TaskID <= next.TaskID)
		} else {
			s.Fail("task keys are not sorted prev: %v, next: %v", prev, next)
		}
	}
}

func (s *taskKeySuite) TestPrev() {
	s.Equal(NewKey(time.Unix(0, 1), 0), NewKey(time.Unix(0, 1), 1).Prev())
	s.Equal(NewKey(time.Unix(0, 0), math.MaxInt64), NewKey(time.Unix(0, 1), 0).Prev())
	s.Equal(NewKey(time.Unix(0, 0), math.MaxInt64-1), NewKey(time.Unix(0, 0), math.MaxInt64).Prev())

	s.Equal(NewKey(time.Unix(0, math.MaxInt64), 0), NewKey(time.Unix(0, math.MaxInt64), 1).Prev())
	s.Equal(NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64), NewKey(time.Unix(0, math.MaxInt64), 0).Prev())
	s.Equal(NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64-1), NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64).Prev())
}

func (s *taskKeySuite) TestNext() {
	s.Equal(NewKey(time.Unix(0, 0), math.MaxInt64), NewKey(time.Unix(0, 0), math.MaxInt64-1).Next())
	s.Equal(NewKey(time.Unix(0, 1), 0), NewKey(time.Unix(0, 0), math.MaxInt64).Next())
	s.Equal(NewKey(time.Unix(0, 1), 1), NewKey(time.Unix(0, 1), 0).Next())

	s.Equal(NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64), NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64-1).Next())
	s.Equal(NewKey(time.Unix(0, math.MaxInt64), 0), NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64).Next())
	s.Equal(NewKey(time.Unix(0, math.MaxInt64), 1), NewKey(time.Unix(0, math.MaxInt64), 0).Next())
}

func (s *taskKeySuite) TestSub() {
	s.Equal(
		NewKey(time.Unix(0, math.MaxInt64).UTC(), 1),
		MaximumKey.Sub(NewKey(time.Unix(0, 0).UTC(), math.MaxInt64-1)),
	)

	s.Equal(
		NewKey(time.Unix(0, math.MaxInt64).UTC(), 0),
		MaximumKey.Sub(NewKey(time.Unix(0, 0).UTC(), math.MaxInt64)),
	)

	s.Equal(
		NewKey(time.Unix(0, 0).UTC(), 1),
		NewKey(time.Unix(0, math.MaxInt64).UTC(), 1).Sub(NewKey(time.Unix(0, math.MaxInt64), 0)),
	)
}
