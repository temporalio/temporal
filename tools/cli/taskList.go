package cli

import "github.com/urfave/cli"

func newTaskListCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "describe",
			Aliases: []string{"desc"},
			Usage:   "Describe pollers info of tasklist",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagTaskListWithAlias,
					Usage: "TaskList description",
				},
				cli.StringFlag{
					Name:  FlagTaskListTypeWithAlias,
					Value: "decision",
					Usage: "Optional TaskList type [decision|activity]",
				},
			},
			Action: func(c *cli.Context) {
				DescribeTaskList(c)
			},
		},
		{
			Name:    "list-partition",
			Aliases: []string{"lp"},
			Usage:   "List all the tasklist partitions and the hostname for partitions.",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagTaskListWithAlias,
					Usage: "TaskList description",
				},
			},
			Action: func(c *cli.Context) {
				ListTaskListPartitions(c)
			},
		},
	}
}
