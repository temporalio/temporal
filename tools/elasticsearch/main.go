package elasticsearch

import (
	"os"

	"github.com/urfave/cli"
	"go.temporal.io/server/common/log"
	commonschema "go.temporal.io/server/tools/common/schema"
)

const (
	CLIOptVisibilityIndex = "index"
	CLIOptAWSCredentials  = "aws-credentials"
	CLIOptAWSToken        = "aws-session-token"
	CLIOptFailSilently    = "fail"

	CLIFlagVisibilityIndex = CLIOptVisibilityIndex + ", i"
	CLIFlagAWSToken        = CLIOptAWSToken
	CLIFlagAWSCredentials  = CLIOptAWSCredentials + ", aws"
	CLIFlagFailSilently    = CLIOptFailSilently
)

// RunTool runs the temporal-elasticsearch-tool command line tool
func RunTool(args []string) error {
	app := BuildCLIOptions()
	return app.Run(args)
}

var osExit = os.Exit

// root handler for all cli commands
func cliHandler(c *cli.Context, handler func(c *cli.Context, logger log.Logger) error, logger log.Logger) {
	quiet := c.GlobalBool(commonschema.CLIOptQuiet)
	err := handler(c, logger)
	if err != nil && !quiet {
		osExit(1)
	}
}

// BuildCLIOptions builds the options for cli
func BuildCLIOptions() *cli.App {

	app := cli.NewApp()
	app.Name = "temporal-elasticsearch-tool"
	app.Usage = "Command line tool for temporal elasticsearch operations (EXPERIMENTAL)"
	app.Version = "0.0.1"

	logger := log.NewCLILogger()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   commonschema.CLIFlagEndpoint,
			Value:  "http://127.0.0.1:9200",
			Usage:  "hostname or ip address of elasticsearch server",
			EnvVar: "ES_SERVER",
		},
		cli.StringFlag{
			Name:   commonschema.CLIFlagUser,
			Value:  "",
			Usage:  "username for elasticsearch or aws_access_key_id if using static aws credentials",
			EnvVar: "ES_USER",
		},
		cli.StringFlag{
			Name:   commonschema.CLIFlagPassword,
			Value:  "",
			Usage:  "password for elasticsearch or aws_secret_access_key if using static aws credentials",
			EnvVar: "ES_PWD",
		},
		cli.StringFlag{
			Name:   CLIFlagAWSCredentials,
			Value:  "",
			Usage:  "AWS credentials provider (supported ['static', 'environment', 'aws-sdk-default'])",
			EnvVar: "AWS_CREDENTIALS",
		},
		cli.StringFlag{
			Name:   CLIFlagAWSToken,
			Value:  "",
			Usage:  "AWS sessiontoken for use with 'static' AWS credentials provider",
			EnvVar: "AWS_SESSION_TOKEN",
		},
		cli.BoolFlag{
			Name:  commonschema.CLIOptQuiet,
			Usage: "don't log errors to stderr",
		},
		cli.BoolFlag{
			Name:   commonschema.CLIFlagEnableTLS,
			Usage:  "enable TLS for elasticsearch connection",
			EnvVar: "ES_TLS",
		},
		cli.StringFlag{
			Name:   commonschema.CLIFlagTLSCertFile,
			Value:  "",
			Usage:  "path to TLS certificate file (tls must be enabled)",
			EnvVar: "ES_TLS_CERT_FILE",
		},
		cli.StringFlag{
			Name:   commonschema.CLIFlagTLSKeyFile,
			Value:  "",
			Usage:  "path to TLS key file (tls must be enabled)",
			EnvVar: "ES_TLS_KEY_FILE",
		},
		cli.StringFlag{
			Name:   commonschema.CLIFlagTLSCaFile,
			Value:  "",
			Usage:  "path to TLS CA certificate file (tls must be enabled)",
			EnvVar: "ES_TLS_CA_FILE",
		},
		cli.BoolFlag{
			Name:   commonschema.CLIFlagTLSDisableHostVerification,
			Usage:  "disable TLS host name verification (tls must be enabled)",
			EnvVar: "ES_TLS_DISABLE_HOST_VERIFICATION",
		},
		cli.StringFlag{
			Name:   commonschema.CLIFlagTLSHostName,
			Value:  "",
			Usage:  "TLS server name for host name verification (tls must be enabled)",
			EnvVar: "ES_TLS_SERVER_NAME",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:  "setup-schema",
			Usage: "setup elasticsearch cluster settings and index template",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  CLIFlagFailSilently,
					Usage: "fail silently on HTTP errors",
				},
			},
			Action: func(c *cli.Context) error {
				cliHandler(c, setupSchema, logger)
				return nil
			},
		},
		{
			Name:  "update-schema",
			Usage: "update elasticsearch index template, and index mappings if --index is specified",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   CLIFlagVisibilityIndex,
					Usage:  "name of the visibility index to update mappings for (optional)",
					EnvVar: "ES_VISIBILITY_INDEX",
				},
				cli.BoolFlag{
					Name:  CLIFlagFailSilently,
					Usage: "fail silently on HTTP errors",
				},
			},
			Action: func(c *cli.Context) error {
				cliHandler(c, updateSchema, logger)
				return nil
			},
		},
		{
			Name:  "create-index",
			Usage: "create elasticsearch visibility index",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     CLIFlagVisibilityIndex,
					Usage:    "name of the visibility index to create",
					Required: true,
					EnvVar:   "ES_VISIBILITY_INDEX",
				},
				cli.BoolFlag{
					Name:  CLIFlagFailSilently,
					Usage: "fail silently on HTTP errors",
				},
			},
			Action: func(c *cli.Context) error {
				cliHandler(c, createIndex, logger)
				return nil
			},
		},
		{
			Name:  "drop-index",
			Usage: "delete elasticsearch visibility index",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     CLIFlagVisibilityIndex,
					Usage:    "name of the visibility index to delete",
					Required: true,
					EnvVar:   "ES_VISIBILITY_INDEX",
				},
				cli.BoolFlag{
					Name:  CLIFlagFailSilently,
					Usage: "fail silently on HTTP errors",
				},
			},
			Action: func(c *cli.Context) error {
				cliHandler(c, dropIndex, logger)
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
