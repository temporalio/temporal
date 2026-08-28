package tdbg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc"
)

const dynamicConfigDumpMaxReceiveSize = 16 << 20

var dynamicConfigDumpNote = fmt.Sprintf(
	"Note: This dump contains configured ConstrainedValues only. It does not include registered setting defaults or resolved effective values. Use `tdbg dc get` to query the effective value used by the server. Dump responses are limited to %d MiB; larger responses cause the command to fail without writing a file.",
	dynamicConfigDumpMaxReceiveSize/(1<<20),
)

const dynamicConfigGetNote = "Note: Constraints not used by this setting are ignored. Use --verbose to inspect the constraint description and configured constrained values."

func newDynamicConfigCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:      "get",
			Usage:     "Get the effective value of one dynamic config key",
			UsageText: "tdbg dynamic-config get [command options]\ntdbg dc get [command options]",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagDynamicConfigKey,
					Aliases:  []string{"k"},
					Usage:    "Dynamic config key",
					Required: true,
				},
				&cli.StringFlag{
					Name:    FlagDynamicConfigConstraints,
					Aliases: []string{"c"},
					Usage:   `JSON object of dynamic config constraints, for example: '{"namespace":"my-namespace"}'`,
				},
				&cli.BoolFlag{
					Name:    FlagVerbose,
					Aliases: []string{"v"},
					Usage:   "Show the key, effective value, query constraints, and configured constrained values as JSON",
				},
			},
			Action: func(c *cli.Context) error {
				return getDynamicConfigValue(c, clientFactory)
			},
		},
		{
			Name:      "dump",
			Usage:     "Dump all configured dynamic config values",
			UsageText: "tdbg dynamic-config dump [command options]\ntdbg dc dump [command options]",
			Action: func(c *cli.Context) error {
				return dumpDynamicConfigValues(c, clientFactory)
			},
		},
	}
}

func getDynamicConfigValue(c *cli.Context, clientFactory ClientFactory) error {
	constraintsJSON := c.String(FlagDynamicConfigConstraints)
	constraints, err := dynamicconfig.ParseConstraintsJSON(constraintsJSON)
	if err != nil {
		return fmt.Errorf("invalid dynamic config constraints: %w", err)
	}

	ctx, cancel := newContext(c)
	defer cancel()
	response, err := clientFactory.AdminClient(c).GetDynamicConfigValue(
		ctx,
		&adminservice.GetDynamicConfigValueRequest{
			Key:                      c.String(FlagDynamicConfigKey),
			Constraints:              constraintsJSON,
			IncludeConstrainedValues: c.Bool(FlagVerbose),
		},
	)
	if err != nil {
		return fmt.Errorf("unable to get dynamic config value: %w", err)
	}
	if strings.TrimSpace(constraintsJSON) != "" {
		if _, err := fmt.Fprintln(c.App.ErrWriter, dynamicConfigGetNote); err != nil {
			return fmt.Errorf("unable to print dynamic config get note: %w", err)
		}
	}
	if c.Bool(FlagVerbose) {
		output, err := json.MarshalIndent(struct {
			Key                   string                    `json:"key"`
			EffectiveValue        json.RawMessage           `json:"effectiveValue"`
			QueryConstraints      dynamicconfig.Constraints `json:"queryConstraints"`
			ConstraintDescription string                    `json:"constraintDescription"`
			ConstrainedValues     json.RawMessage           `json:"constrainedValues"`
		}{
			Key:                   c.String(FlagDynamicConfigKey),
			EffectiveValue:        response.GetValue(),
			QueryConstraints:      constraints,
			ConstraintDescription: response.GetConstraintDescription(),
			ConstrainedValues:     response.GetConstrainedValues(),
		}, "", "  ")
		if err != nil {
			return fmt.Errorf("unable to format dynamic config value: %w", err)
		}
		_, err = fmt.Fprintln(c.App.Writer, string(output))
		return err
	}
	_, err = fmt.Fprintln(c.App.Writer, string(response.GetValue()))
	return err
}

func dumpDynamicConfigValues(c *cli.Context, clientFactory ClientFactory) error {
	if _, err := fmt.Fprintln(c.App.ErrWriter, dynamicConfigDumpNote); err != nil {
		return fmt.Errorf("unable to print dynamic config dump note: %w", err)
	}

	ctx, cancel := newContext(c)
	defer cancel()
	response, err := clientFactory.AdminClient(c).DumpDynamicConfigValues(
		ctx,
		&adminservice.DumpDynamicConfigValuesRequest{},
		grpc.MaxCallRecvMsgSize(dynamicConfigDumpMaxReceiveSize),
	)
	if err != nil {
		return fmt.Errorf("unable to dump dynamic config values: %w", err)
	}

	var output bytes.Buffer
	if err := json.Indent(&output, response.GetValues(), "", "  "); err != nil {
		return fmt.Errorf("unable to format dynamic config values: %w", err)
	}
	if err := output.WriteByte('\n'); err != nil {
		return fmt.Errorf("unable to format dynamic config values: %w", err)
	}

	filename := fmt.Sprintf("tmp_dc_cvs_%s.json", time.Now().UTC().Format("20060102T150405Z"))
	if err := os.WriteFile(filename, output.Bytes(), 0o644); err != nil {
		return fmt.Errorf("unable to write dynamic config values to %q: %w", filename, err)
	}
	_, err = fmt.Fprintln(c.App.Writer, filename)
	return err
}
