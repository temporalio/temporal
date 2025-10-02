package chasm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/payload"
)

func TestVisibilityValue(t *testing.T) {
	t.Run("Int", func(t *testing.T) {
		v := VisibilityValueInt(42)
		p := v.MustEncode()
		require.NotNil(t, p)

		var out int
		err := payload.Decode(p, &out)
		require.NoError(t, err)
		require.Equal(t, 42, out)

		require.True(t, v.Equal(VisibilityValueInt(42)))
		require.False(t, v.Equal(VisibilityValueInt(43)))
		// Different underlying type should not be equal even if numerically same
		require.False(t, v.Equal(VisibilityValueInt32(42)))
	})

	t.Run("Int32", func(t *testing.T) {
		v := VisibilityValueInt32(123)
		p := v.MustEncode()
		require.NotNil(t, p)

		var out int32
		err := payload.Decode(p, &out)
		require.NoError(t, err)
		require.Equal(t, int32(123), out)

		require.True(t, v.Equal(VisibilityValueInt32(123)))
		require.False(t, v.Equal(VisibilityValueInt32(124)))
		require.False(t, v.Equal(VisibilityValueInt64(123)))
	})

	t.Run("Int64", func(t *testing.T) {
		v := VisibilityValueInt64(9876543210)
		p := v.MustEncode()
		require.NotNil(t, p)

		var out int64
		err := payload.Decode(p, &out)
		require.NoError(t, err)
		require.Equal(t, int64(9876543210), out)

		require.True(t, v.Equal(VisibilityValueInt64(9876543210)))
		require.False(t, v.Equal(VisibilityValueInt64(9876543211)))
		require.False(t, v.Equal(VisibilityValueInt(9876543210)))
	})

	t.Run("String", func(t *testing.T) {
		v := VisibilityValueString("hello, 世界")
		p := v.MustEncode()
		require.NotNil(t, p)

		var out string
		err := payload.Decode(p, &out)
		require.NoError(t, err)
		require.Equal(t, "hello, 世界", out)

		require.True(t, v.Equal(VisibilityValueString("hello, 世界")))
		require.False(t, v.Equal(VisibilityValueString("hello")))
		require.False(t, v.Equal(VisibilityValueBool(true)))
	})

	t.Run("Bool", func(t *testing.T) {
		v := VisibilityValueBool(true)
		p := v.MustEncode()
		require.NotNil(t, p)

		var out bool
		err := payload.Decode(p, &out)
		require.NoError(t, err)
		require.Equal(t, true, out)

		require.True(t, v.Equal(VisibilityValueBool(true)))
		require.False(t, v.Equal(VisibilityValueBool(false)))
		require.False(t, v.Equal(VisibilityValueString("true")))
	})

	t.Run("Float64", func(t *testing.T) {
		v := VisibilityValueFloat64(3.14159)
		p := v.MustEncode()
		require.NotNil(t, p)

		var out float64
		err := payload.Decode(p, &out)
		require.NoError(t, err)
		require.InDelta(t, 3.14159, out, 1e-9)

		require.True(t, v.Equal(VisibilityValueFloat64(3.14159)))
		require.False(t, v.Equal(VisibilityValueFloat64(2.71828)))
		require.False(t, v.Equal(VisibilityValueInt(3)))
	})

	t.Run("ByteSlice", func(t *testing.T) {
		v := VisibilityValueByteSlice([]byte{0x01, 0x02, 0x03})
		p := v.MustEncode()
		require.NotNil(t, p)

		var out []byte
		err := payload.Decode(p, &out)
		require.NoError(t, err)
		require.Equal(t, []byte{0x01, 0x02, 0x03}, out)

		require.True(t, v.Equal(VisibilityValueByteSlice([]byte{0x01, 0x02, 0x03})))
		require.False(t, v.Equal(VisibilityValueByteSlice([]byte{0x01, 0x02})))
		require.False(t, v.Equal(VisibilityValueByteSlice([]byte{0x01, 0x03, 0x02})))
		require.False(t, v.Equal(VisibilityValueString("\x01\x02\x03")))
	})

	t.Run("StringSlice", func(t *testing.T) {
		v := VisibilityValueStringSlice([]string{"a", "b", "c"})
		p := v.MustEncode()
		require.NotNil(t, p)

		var out []string
		err := payload.Decode(p, &out)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b", "c"}, out)

		require.True(t, v.Equal(VisibilityValueStringSlice([]string{"a", "b", "c"})))
		require.False(t, v.Equal(VisibilityValueStringSlice([]string{"a", "c", "b"})))
		require.False(t, v.Equal(VisibilityValueStringSlice([]string{"a", "b"})))
		require.False(t, v.Equal(VisibilityValueString("[a b c]")))
	})

	// Time
	t.Run("Time", func(t *testing.T) {
		// Use a fixed UTC time for deterministic comparison
		base := time.Date(2025, 9, 28, 12, 34, 56, 789000000, time.UTC)
		v := VisibilityValueTime(base)
		p := v.MustEncode()
		require.NotNil(t, p)

		var out time.Time
		err := payload.Decode(p, &out)
		require.NoError(t, err)
		require.True(t, base.Equal(out))

		require.True(t, v.Equal(VisibilityValueTime(base)))
		require.False(t, v.Equal(VisibilityValueTime(base.Add(time.Second))))
		require.False(t, v.Equal(VisibilityValueString(base.String())))
	})
}

func TestIsVisibilityValueEqual(t *testing.T) {
	// nil vs nil
	require.True(t, isVisibilityValueEqual(nil, nil))

	// one nil
	require.False(t, isVisibilityValueEqual(VisibilityValueInt(1), nil))
	require.False(t, isVisibilityValueEqual(nil, VisibilityValueInt(1)))

	// equal values
	require.True(t, isVisibilityValueEqual(VisibilityValueString("x"), VisibilityValueString("x")))
	require.True(t, isVisibilityValueEqual(VisibilityValueInt64(5), VisibilityValueInt64(5)))

	// not equal values
	require.False(t, isVisibilityValueEqual(VisibilityValueInt(5), VisibilityValueInt(6)))
	require.False(t, isVisibilityValueEqual(VisibilityValueInt(5), VisibilityValueInt64(5)))
}
