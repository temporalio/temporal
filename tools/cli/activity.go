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

package cli

import (
	"github.com/urfave/cli"
)

func newActivityCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "complete",
			Aliases: []string{"comp"},
			Usage:   "complete an activity",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowId",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunId",
				},
				cli.StringFlag{
					Name:  FlagActivityIDWithAlias,
					Usage: "The activityId to operate on",
				},
				cli.StringFlag{
					Name:  FlagResult,
					Usage: "Result of the activity",
				},
				cli.StringFlag{
					Name:  FlagIdentity,
					Usage: "Identity of the operator",
				},
			},
			Action: func(c *cli.Context) {
				CompleteActivity(c)
			},
		},
		{
			Name:  "fail",
			Usage: "fail an activity",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowId",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunId",
				},
				cli.StringFlag{
					Name:  FlagActivityIDWithAlias,
					Usage: "The activityId to operate on",
				},
				cli.StringFlag{
					Name:  FlagReason,
					Usage: "Reason to fail the activity",
				},
				cli.StringFlag{
					Name:  FlagDetail,
					Usage: "Detail to fail the activity",
				},
				cli.StringFlag{
					Name:  FlagIdentity,
					Usage: "Identity of the operator",
				},
			},
			Action: func(c *cli.Context) {
				FailActivity(c)
			},
		},
	}
}
