package tdbg_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
)

func (s *utilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}
func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(utilSuite))
}

type utilSuite struct {
	*require.Assertions
	suite.Suite
}

// TestAcceptStringSliceArgsWithCommas tests that the cli accepts string slice args with commas
// If the test fails consider downgrading urfave/cli/v2 to v2.4.0
// See https://github.com/urfave/cli/pull/1241
func (s *utilSuite) TestAcceptStringSliceArgsWithCommas() {
	app := cli.NewApp()
	app.Name = "testapp"
	app.Commands = []*cli.Command{
		{
			Name: "dostuff",
			Action: func(c *cli.Context) error {
				input := c.StringSlice("input")
				s.Equal(3, len(input))
				expectedInput := []string{"s1", "s2", "s3"}
				s.Equal(expectedInput, input)
				return nil
			},
			Flags: []cli.Flag{
				&cli.StringSliceFlag{
					Name: "input",
				},
			},
		},
	}
	s.NoError(app.Run([]string{"testapp", "dostuff",
		"--input", `s1,s2`,
		"--input", `s3`}))
}
