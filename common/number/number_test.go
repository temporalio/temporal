package number

import (
	"math/rand"
	"testing"

	"go.temporal.io/server/common/testing/parallelsuite"
)

type (
	numberSuite struct {
		parallelsuite.Suite[*numberSuite]
	}
)

func TestNumberSuite(t *testing.T) {
	parallelsuite.Run(t, new(numberSuite))
}

func (s *numberSuite) TestInt() {
	number := rand.Intn(128)
	for _, n := range []any{
		int8(number),
		int16(number),
		int32(number),
		int64(number),
		int(number),
	} {
		s.Equal(float64(number), NewNumber(n).GetFloatOrDefault(rand.Float64()))
		s.Equal(int(number), NewNumber(n).GetIntOrDefault(rand.Int()))
		s.Equal(uint(number), NewNumber(n).GetUintOrDefault(uint(rand.Uint64())))
	}
}

func (s *numberSuite) TestUint() {
	number := rand.Intn(256)
	for _, n := range []any{
		uint8(number),
		uint16(number),
		uint32(number),
		uint64(number),
		uint(number),
	} {
		s.Equal(float64(number), NewNumber(n).GetFloatOrDefault(rand.Float64()))
		s.Equal(int(number), NewNumber(n).GetIntOrDefault(rand.Int()))
		s.Equal(uint(number), NewNumber(n).GetUintOrDefault(uint(rand.Uint64())))
	}
}

func (s *numberSuite) TestFloat() {
	number := rand.Float32() * float32(rand.Int())
	for _, n := range []any{
		float32(number),
		float64(number),
	} {
		s.Equal(float64(number), NewNumber(n).GetFloatOrDefault(rand.Float64()))
		s.Equal(int(number), NewNumber(n).GetIntOrDefault(rand.Int()))
		s.Equal(uint(number), NewNumber(n).GetUintOrDefault(uint(rand.Uint64())))
	}
}
