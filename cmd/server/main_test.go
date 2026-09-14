package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

// TestStart_GlobalFlagAfterSubcommand_Helpful asserts that when a user passes
// a global flag (such as `-c`, `--config`, `-e`, `--env`, `-r`, `--root`,
// `--zone`, `--config-file`) AFTER the `start` subcommand, the resulting
// error message points them at the correct placement instead of the bare
// "flag provided but not defined" string from urfave/cli.
//
// See https://github.com/temporalio/temporal/issues/6226.
func TestStart_GlobalFlagAfterSubcommand_Helpful(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "short config", args: []string{"temporal", "start", "-c", "/etc/temporal"}},
		{name: "long config", args: []string{"temporal", "start", "--config", "/etc/temporal"}},
		{name: "short env", args: []string{"temporal", "start", "-e", "docker"}},
		{name: "long env", args: []string{"temporal", "start", "--env", "docker"}},
		{name: "short root", args: []string{"temporal", "start", "-r", "/etc"}},
		{name: "long root", args: []string{"temporal", "start", "--root", "/etc"}},
		{name: "zone alias", args: []string{"temporal", "start", "--az", "us-east-1a"}},
		{name: "long zone", args: []string{"temporal", "start", "--zone", "us-east-1a"}},
		{name: "config-file", args: []string{"temporal", "start", "--config-file", "/etc/temporal/cfg.yaml"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := buildCLI()
			app.Writer = &bytes.Buffer{}
			app.ErrWriter = &bytes.Buffer{}

			err := app.Run(tc.args)

			require.Error(t, err)
			require.Contains(t, err.Error(), "is a global flag",
				"expected a helpful message naming the global flag, got %q", err.Error())
			require.Contains(t, err.Error(), "before `start`",
				"expected guidance about placement before the start subcommand, got %q", err.Error())
		})
	}
}

// TestStart_UnknownFlag_PreservesOriginalError asserts that unrelated unknown
// flags still return the original urfave/cli error (we only rewrite when the
// flag actually maps to a known global flag).
func TestStart_UnknownFlag_PreservesOriginalError(t *testing.T) {
	app := buildCLI()
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}

	err := app.Run([]string{"temporal", "start", "--not-a-real-flag", "x"})

	require.Error(t, err)
	require.Contains(t, err.Error(), "flag provided but not defined")
	require.NotContains(t, err.Error(), "is a global flag")
}

// TestRewriteAsGlobalFlagPlacementError exercises the unit-level helper for
// the cases we care about: non-matching errors pass through, and an error
// that names a flag not defined globally also passes through.
func TestRewriteAsGlobalFlagPlacementError(t *testing.T) {
	app := cli.NewApp()
	app.Name = "temporal"
	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "config", Aliases: []string{"c"}},
	}
	cCtx := cli.NewContext(app, nil, nil)

	t.Run("non-usage error passes through", func(t *testing.T) {
		orig := errors.New("some other error")
		got, rewritten := rewriteAsGlobalFlagPlacementError(cCtx, orig)
		require.False(t, rewritten)
		require.Same(t, orig, got)
	})

	t.Run("usage error for unknown flag passes through", func(t *testing.T) {
		orig := errors.New("flag provided but not defined: -bogus")
		got, rewritten := rewriteAsGlobalFlagPlacementError(cCtx, orig)
		require.False(t, rewritten)
		require.Same(t, orig, got)
	})

	t.Run("usage error for known global flag is rewritten", func(t *testing.T) {
		orig := errors.New("flag provided but not defined: -c")
		got, rewritten := rewriteAsGlobalFlagPlacementError(cCtx, orig)
		require.True(t, rewritten)
		require.NotSame(t, orig, got)
		require.Contains(t, got.Error(), "--config is a global flag")
	})
}

// TestHelpfulSubcommandFlagError_NilInputs exercises the nil-safety of the
// top-level OnUsageError callback.
func TestHelpfulSubcommandFlagError_NilInputs(t *testing.T) {
	app := cli.NewApp()
	cCtx := cli.NewContext(app, nil, nil)
	require.NoError(t, helpfulSubcommandFlagError(cCtx, nil, true))
}
