package serverfailpoints_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/serverfailpoints"
)

func TestGRPCMethod(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		keyName    string
		wantMethod string
		wantOK     bool
	}{
		"request key":  {keyName: serverfailpoints.GRPCKey("/pkg.Service/Method").Name(), wantMethod: "/pkg.Service/Method", wantOK: true},
		"response key": {keyName: serverfailpoints.GRPCResponseKey("/pkg.Service/Method").Name(), wantMethod: "/pkg.Service/Method", wantOK: true},
		"HTTP key":     {keyName: serverfailpoints.HTTPKey("POST /path").Name()},
		"any key":      {keyName: serverfailpoints.AnyGRPCKey.Name()},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			method, ok := serverfailpoints.GRPCMethod(tc.keyName)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantMethod, method)
		})
	}
}
