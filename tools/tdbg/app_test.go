// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tdbg_test

import (
	"encoding/json"
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
				s.Equal(2, len(c.StringSlice("input")))
				for _, inp := range c.StringSlice("input") {
					var thing any
					s.NoError(json.Unmarshal([]byte(inp), &thing))
				}
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
		"--input", `{"field1": 34, "field2": false}`,
		"--input", `{"numbers": [4,5,6]}`}))
}
