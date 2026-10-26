package sqlite

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
)

func TestDriverDSNPlatformSelection(t *testing.T) {
	for _, test := range []struct {
		name       string
		attributes map[string]string
		native     string
		wasi       string
	}{
		{"file", nil, "file:example?_time_format=sqlite", "file:example?_timefmt=auto"},
		{"memory", map[string]string{"mode": "memory", "cache": "shared"}, "file:example?_time_format=sqlite&cache=shared&mode=memory", "file:/example?_timefmt=auto&vfs=memdb"},
		{"explicit VFS", map[string]string{"mode": "memory", "vfs": "custom"}, "file:example?_time_format=sqlite&mode=memory&vfs=custom", "file:example?_timefmt=auto&mode=memory&vfs=custom"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dsn, err := buildDSN(&config.SQL{DatabaseName: "example", ConnectAttributes: test.attributes})
			require.NoError(t, err)
			want := test.native
			if runtime.GOOS == "wasip1" {
				want = test.wasi
			}
			require.Equal(t, want, dsn)
		})
	}
	_, err := buildDSN(&config.SQL{DatabaseName: "example", ConnectAttributes: map[string]string{" mode ": "memory", "mode": "ro"}})
	require.Error(t, err)
}
