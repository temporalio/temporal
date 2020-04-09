package cli

import "github.com/urfave/cli"

func newClusterCommands() []cli.Command {
	return []cli.Command{
		{
			Name:  "get-search-attr",
			Usage: "get list of legal search attributes that can be used in list workflow query.",
			Action: func(c *cli.Context) {
				GetSearchAttributes(c)
			},
		},
	}
}
