package number

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type compact8Suite struct {
	suite.Suite
	*require.Assertions
}

func TestCompact8Suite(t *testing.T) {
	suite.Run(t, new(compact8Suite))
}

func (s *compact8Suite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *compact8Suite) TestDecodeKnownValues() {
	s.Equal(int64(0), DecodeCompact8(0))
	// Subnormals: e=0, m << 5
	s.Equal(int64(32), DecodeCompact8(1))    // 1 << 5
	s.Equal(int64(64), DecodeCompact8(2))    // 2 << 5
	s.Equal(int64(352), DecodeCompact8(11))  // 11 << 5
	// e=1 (bytes 12-23): (12+m) << 5
	s.Equal(int64(384), DecodeCompact8(12))  // 12 << 5
	s.Equal(int64(736), DecodeCompact8(23))  // 23 << 5
	// e=2 (bytes 24-35): (12+m) << 6
	s.Equal(int64(768), DecodeCompact8(24))  // 12 << 6
	s.Equal(int64(1472), DecodeCompact8(35)) // 23 << 6
	// Max: byte 255 = e=21, m=3: 15 << 25 = 503316480
	s.Equal(int64(15)<<25, DecodeCompact8(255))
	s.Equal(int64(503316480), DecodeCompact8(255))
}

func (s *compact8Suite) TestEncodeKnownValues() {
	s.Equal(Compact8(0), EncodeCompact8(0))
	s.Equal(Compact8(0), EncodeCompact8(-1))
	s.Equal(Compact8(0), EncodeCompact8(31))   // below smallest subnormal
	s.Equal(Compact8(1), EncodeCompact8(32))   // smallest subnormal
	s.Equal(Compact8(1), EncodeCompact8(50))   // rounds down to 32
	s.Equal(Compact8(2), EncodeCompact8(64))   // subnormal m=2
	s.Equal(Compact8(12), EncodeCompact8(384)) // first normalized
	s.Equal(Compact8(24), EncodeCompact8(768)) // e=2, m=0
	s.Equal(Compact8(255), EncodeCompact8(1e15))
}

func (s *compact8Suite) TestEncodeRoundsDown() {
	// 400 is between 384 (byte 12) and 416 (byte 13)
	s.Equal(Compact8(12), EncodeCompact8(400))
	s.Equal(int64(384), DecodeCompact8(EncodeCompact8(400)))
	// 415 still rounds down to 384
	s.Equal(Compact8(12), EncodeCompact8(415))
}

func (s *compact8Suite) TestRoundtrip() {
	for b := 0; b < 256; b++ {
		decoded := DecodeCompact8(Compact8(b))
		reencoded := EncodeCompact8(decoded)
		s.Equalf(Compact8(b), reencoded, "byte=%d decoded=%d", b, decoded)
	}
}

func (s *compact8Suite) TestMonotonic() {
	prev := int64(0)
	for b := 0; b < 256; b++ {
		v := DecodeCompact8(Compact8(b))
		s.GreaterOrEqualf(v, prev, "byte=%d", b)
		prev = v
	}
}

func (s *compact8Suite) TestStrictlyIncreasing() {
	// Every byte should decode to a strictly larger value than the previous
	prev := int64(-1)
	for b := 0; b < 256; b++ {
		v := DecodeCompact8(Compact8(b))
		s.Greaterf(v, prev, "byte=%d", b)
		prev = v
	}
}

func (s *compact8Suite) TestRoundDown() {
	for b := 0; b < 255; b++ {
		decoded := DecodeCompact8(Compact8(b))
		nextDecoded := DecodeCompact8(Compact8(b + 1))
		if nextDecoded <= decoded+1 {
			continue // consecutive integers, can't test rounding
		}
		mid := decoded + (nextDecoded-decoded)/2
		encoded := EncodeCompact8(mid)
		s.Equalf(Compact8(b), encoded, "value=%d should round down to byte=%d (not %d)", mid, b, encoded)
	}
}

func (s *compact8Suite) TestUpdateSameBucket() {
	code := EncodeCompact8(768) // e=2, m=0
	s.Equal(code, UpdateCompact8(768, code))
	s.Equal(code, UpdateCompact8(800, code))
	s.Equal(code, UpdateCompact8(831, code))
}

func (s *compact8Suite) TestUpdateHysteresis() {
	code768 := EncodeCompact8(768) // e=2, m=0 (byte 24)
	code736 := EncodeCompact8(736) // e=1, m=11 (byte 23)
	s.Equal(int64(768), DecodeCompact8(code768))
	s.Equal(int64(736), DecodeCompact8(code736))

	// gap=32, margin=16 (half step at e=1), so transition from code768 down at value<736+16=752.
	// Actually: newDist = value - 736, oldDist = 768 - value
	// Switch when newDist < oldDist - margin, i.e. value-736 < 768-value-16
	// i.e. 2*value < 768+736-16 = 1488, value < 744

	// From code768: small drops stay (hysteresis keeps it sticky)
	s.Equal(code768, UpdateCompact8(750, code768))
	s.Equal(code768, UpdateCompact8(744, code768)) // boundary: stays

	// From code768: large enough drop switches
	s.Equal(code736, UpdateCompact8(743, code768))
	s.Equal(code736, UpdateCompact8(736, code768))

	// From code736: values below 768 still encode to code736, no change
	s.Equal(code736, UpdateCompact8(767, code736))
	s.Equal(code736, UpdateCompact8(750, code736))

	// From code736: value reaches next bucket, switches up
	s.Equal(code768, UpdateCompact8(768, code736))
}

func (s *compact8Suite) TestUpdateLargeJump() {
	code := EncodeCompact8(736)
	result := UpdateCompact8(100000, code)
	s.Equal(EncodeCompact8(100000), result)
}

func (s *compact8Suite) TestUpdateZeroBoundary() {
	s.Equal(EncodeCompact8(768), UpdateCompact8(768, 0))
	s.Equal(Compact8(0), UpdateCompact8(0, EncodeCompact8(768)))
}
