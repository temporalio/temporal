package flakereport

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/tools/common/github"
)

func TestGenerateCommandRPSFlag(t *testing.T) {
	for _, flag := range NewCliApp().Commands[0].Flags {
		intFlag, ok := flag.(*cli.IntFlag)
		if !ok || intFlag.Name != "rps" {
			continue
		}
		require.Equal(t, github.DefaultAPIRPS, intFlag.Value)
		return
	}
	t.Fatal("generate command does not define an rps flag")
}
