package elasticsearch

import (
	"os"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tools/common/schema"
)

const (
	CLIOptESVersion       = "es-version"
	CLIOptESURL           = "endpoint"
	CLIOptESUsername      = "user"
	CLIOptESPassword      = "password"
	CLIOptVisibilityIndex = "index"
	CLIOptAWSCredentials  = "aws-credentials"
	CLIOptAWSToken        = "aws-session-token"
	CLIOptSettingsFile    = "settings-file"
	CLIOptTemplateFile    = "template-file"
	CLIOptFailSilently    = "fail"

	CLIFlagESVersion       = CLIOptESVersion
	CLIFlagESURL           = CLIOptESURL + ", e"
	CLIFlagESUsername      = CLIOptESUsername + ", u"
	CLIFlagESPassword      = CLIOptESPassword + ", p"
	CLIFlagAWSToken        = CLIOptAWSToken
	CLIFlagVisibilityIndex = CLIOptVisibilityIndex + ", i"
	CLIFlagAWSCredentials  = CLIOptAWSCredentials + ", aws"
	CLIFlagFailSilently    = CLIOptFailSilently
	CLIFlagSettingsFile    = CLIOptSettingsFile
	CLIFlagTemplateFile    = CLIOptTemplateFile
)

// RunTool runs the temporal-elasticsearch-tool command line tool
func RunTool(args []string) error {
	app := BuildCLIOptions()
	return app.Run(args)
}

var osExit = os.Exit

// root handler for all cli commands
func cliHandler(c *cli.Context, handler func(c *cli.Context, logger log.Logger) error, logger log.Logger) {
	quiet := c.Bool(schema.CLIOptQuiet)
	err := handler(c, logger)
	if err != nil && !quiet {
		osExit(1)
	}
}

// BuildCLIOptions builds the options for cli
func BuildCLIOptions() *cli.App {

	app := cli.NewApp()
	app.Name = "temporal-elasticsearch-tool"
	app.Usage = "Command line tool for temporal elasticsearch operations"
	app.Version = "0.0.1"

	logger := log.NewCLILogger()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    CLIFlagESURL,
			Value:   "http://127.0.0.1:9200",
			Usage:   "hostname or ip address of elasticsearch server",
			EnvVars: []string{"ES_SERVER"},
		},
		&cli.StringFlag{
			Name:    CLIFlagESUsername,
			Value:   "",
			Usage:   "username for elasticsearch or aws_access_key_id if using static aws credentials",
			EnvVars: []string{"ES_USER"},
		},
		&cli.StringFlag{
			Name:    CLIFlagESPassword,
			Value:   "",
			Usage:   "password for elasticsearch or aws_secret_access_key if using static aws credentials",
			EnvVars: []string{"ES_PWD"},
		},
		&cli.StringFlag{
			Name:    CLIFlagESVersion,
			Value:   "v7",
			Usage:   "elasticsearch version",
			EnvVars: []string{"ES_VERSION"},
		},
		&cli.StringFlag{
			Name:    CLIFlagAWSCredentials,
			Value:   "",
			Usage:   "AWS credentials provider (supported ['static', 'environment', 'aws-sdk-default'])",
			EnvVars: []string{"AWS_CREDENTIALS"},
		},
		&cli.StringFlag{
			Name:    CLIFlagAWSToken,
			Value:   "",
			Usage:   "AWS sessiontoken for use with 'static' AWS credentials provider",
			EnvVars: []string{"AWS_SESSION_TOKEN"},
		},
		&cli.BoolFlag{
			Name:  schema.CLIOptQuiet,
			Usage: "don't log errors to stderr",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:    "setup-schema",
			Aliases: []string{"setup"},
			Usage:   "setup initial version of elasticsearch schema and index",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  CLIFlagSettingsFile,
					Usage: "path to the .json cluster settings file",
				},
				&cli.StringFlag{
					Name:  CLIFlagTemplateFile,
					Usage: "path to the .json index template file",
				},
				&cli.StringFlag{
					Name:  CLIFlagVisibilityIndex,
					Usage: "name of the visibility index to create",
				},
				&cli.BoolFlag{
					Name:  CLIFlagFailSilently,
					Usage: "fail silently on HTTP errors",
				},
			},
			Action: func(c *cli.Context) error {
				cliHandler(c, setup, logger)
				return nil
			},
		},
		{
			Name:  "ping",
			Usage: "pings the elasticsearch host",
			Flags: []cli.Flag{},
			Action: func(c *cli.Context) error {
				cliHandler(c, ping, logger)
				return nil
			},
		},
	}

	return app
}
