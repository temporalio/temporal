package tdbg

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

const dynamicConfigDumpMaxReceiveSize = 16 << 20

var dynamicConfigDumpNote = fmt.Sprintf(
	"Note: This YAML dump contains configured ConstrainedValues only and can be read by the file-based dynamic config client. It does not include registered setting defaults or resolved effective values. Use `tdbg dc get` to query the effective value used by the server. Dump responses are limited to %d MiB; larger responses cause the command to fail without writing a file.",
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
					Usage:   `YAML mapping of dynamic config constraints, for example: '{namespace: my-namespace}'`,
				},
				&cli.BoolFlag{
					Name:    FlagVerbose,
					Aliases: []string{"v"},
					Usage:   "Show the key, effective value, query constraints, and configured constrained values as YAML",
				},
			},
			Action: func(c *cli.Context) error {
				return getDynamicConfigValue(c, clientFactory)
			},
		},
		{
			Name:      "describe",
			Usage:     "Describe one dynamic config setting",
			UsageText: "tdbg dynamic-config describe [command options]\ntdbg dc describe [command options]",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagDynamicConfigKey,
					Aliases:  []string{"k"},
					Usage:    "Dynamic config key",
					Required: true,
				},
			},
			Action: func(c *cli.Context) error {
				return describeDynamicConfigSetting(c, clientFactory)
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

func describeDynamicConfigSetting(c *cli.Context, clientFactory ClientFactory) error {
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := clientFactory.AdminClient(c).DescribeDynamicConfigSetting(
		ctx,
		&adminservice.DescribeDynamicConfigSettingRequest{Key: c.String(FlagDynamicConfigKey)},
	)
	if err != nil {
		return fmt.Errorf("unable to describe dynamic config setting: %w", err)
	}

	output, err := yaml.Marshal(struct {
		Key                   string `yaml:"key"`
		ValueType             string `yaml:"valueType"`
		ConstraintDescription string `yaml:"constraintDescription"`
	}{
		Key:                   response.GetKey(),
		ValueType:             response.GetValueType(),
		ConstraintDescription: response.GetConstraintDescription(),
	})
	if err != nil {
		return fmt.Errorf("unable to format dynamic config setting description: %w", err)
	}
	_, err = c.App.Writer.Write(output)
	return err
}

func getDynamicConfigValue(c *cli.Context, clientFactory ClientFactory) error {
	constraintsYAML := c.String(FlagDynamicConfigConstraints)
	_, err := dynamicconfig.ParseConstraintsYAML(constraintsYAML)
	if err != nil {
		return fmt.Errorf("invalid dynamic config constraints: %w", err)
	}

	ctx, cancel := newContext(c)
	defer cancel()
	response, err := clientFactory.AdminClient(c).GetDynamicConfigValue(
		ctx,
		&adminservice.GetDynamicConfigValueRequest{
			Key:                      c.String(FlagDynamicConfigKey),
			Constraints:              constraintsYAML,
			IncludeConstrainedValues: c.Bool(FlagVerbose),
		},
	)
	if err != nil {
		return fmt.Errorf("unable to get dynamic config value: %w", err)
	}
	if strings.TrimSpace(constraintsYAML) != "" {
		if _, err := fmt.Fprintln(c.App.ErrWriter, dynamicConfigGetNote); err != nil {
			return fmt.Errorf("unable to print dynamic config get note: %w", err)
		}
	}
	if c.Bool(FlagVerbose) {
		queryConstraints := any(map[string]any{})
		if strings.TrimSpace(constraintsYAML) != "" {
			if err := yaml.Unmarshal([]byte(constraintsYAML), &queryConstraints); err != nil {
				return fmt.Errorf("unable to format dynamic config constraints: %w", err)
			}
		}
		effectiveValue, err := unmarshalDynamicConfigYAML(response.GetValue())
		if err != nil {
			return fmt.Errorf("unable to format effective dynamic config value: %w", err)
		}
		constrainedValues, err := unmarshalDynamicConfigYAML(response.GetConstrainedValues())
		if err != nil {
			return fmt.Errorf("unable to format constrained dynamic config values: %w", err)
		}
		output, err := yaml.Marshal(struct {
			Key                   string `yaml:"key"`
			QueryConstraints      any    `yaml:"queryConstraints"`
			ConstraintDescription string `yaml:"constraintDescription"`
			EffectiveValue        any    `yaml:"effectiveValue"`
			ConstrainedValues     any    `yaml:"constrainedValues"`
		}{
			Key:                   c.String(FlagDynamicConfigKey),
			QueryConstraints:      queryConstraints,
			ConstraintDescription: response.GetConstraintDescription(),
			EffectiveValue:        effectiveValue,
			ConstrainedValues:     constrainedValues,
		})
		if err != nil {
			return fmt.Errorf("unable to format dynamic config value: %w", err)
		}
		_, err = c.App.Writer.Write(output)
		return err
	}
	if _, err := c.App.Writer.Write(response.GetValue()); err != nil {
		return err
	}
	if !strings.HasSuffix(string(response.GetValue()), "\n") {
		_, err = fmt.Fprintln(c.App.Writer)
	}
	return err
}

func unmarshalDynamicConfigYAML(encodedValue []byte) (any, error) {
	var value any
	if err := yaml.Unmarshal(encodedValue, &value); err != nil {
		return nil, err
	}
	return value, nil
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

	filename := fmt.Sprintf("tmp_dc_cvs_%s.yaml", time.Now().UTC().Format("20060102T150405Z"))
	if err := os.WriteFile(filename, response.GetValues(), 0o644); err != nil {
		return fmt.Errorf("unable to write dynamic config values to %q: %w", filename, err)
	}
	_, err = fmt.Fprintln(c.App.Writer, filename)
	return err
}
