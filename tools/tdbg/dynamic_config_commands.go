package tdbg

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/fatih/color"
	"github.com/google/uuid"
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
const dynamicConfigConstraintAliasNote = `Note: Constraint aliases: "taskType" -> runtime "TaskQueueType"; "historyTaskType" -> runtime "TaskType".`

var (
	dynamicConfigTaskTypeDescriptionPattern = regexp.MustCompile(
		`(^|[,{[:space:]][[:space:]]*)TaskType:`,
	)
	dynamicConfigTaskQueueTypeDescriptionPattern = regexp.MustCompile(
		`(^|[,{[:space:]][[:space:]]*)TaskQueueType:`,
	)
)

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
		ConstraintDescription: formatConstraintDescriptionForYAML(response.GetConstraintDescription()),
	})
	if err != nil {
		return fmt.Errorf("unable to format dynamic config setting description: %w", err)
	}
	if err = printDynamicConfigNotes(c, dynamicConfigConstraintAliasNote); err != nil {
		return err
	}
	if _, err = c.App.Writer.Write(output); err != nil {
		return err
	}
	return nil
}

func getDynamicConfigValue(c *cli.Context, clientFactory ClientFactory) error {
	constraintsJSON := c.String(FlagDynamicConfigConstraints)
	_, err := dynamicconfig.ParseAliasedConstraintsJSON(constraintsJSON)
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
	if c.Bool(FlagVerbose) {
		output := formatVerboseDynamicConfigValue(
			c.String(FlagDynamicConfigKey),
			constraintsJSON,
			response,
		)
		notes := []string{dynamicConfigConstraintAliasNote}
		if strings.TrimSpace(constraintsJSON) != "" {
			notes = append(notes, dynamicConfigGetNote)
		}
		if err = printDynamicConfigNotes(c, notes...); err != nil {
			return err
		}
		if _, err = c.App.Writer.Write([]byte(output)); err != nil {
			return err
		}
		return nil
	}
	if strings.TrimSpace(constraintsJSON) != "" {
		if err = printDynamicConfigNotes(c, dynamicConfigGetNote); err != nil {
			return err
		}
	}
	if _, err := c.App.Writer.Write(response.GetValue()); err != nil {
		return err
	}
	if !strings.HasSuffix(string(response.GetValue()), "\n") {
		if _, err = fmt.Fprintln(c.App.Writer); err != nil {
			return err
		}
	}
	return nil
}

func formatVerboseDynamicConfigValue(
	key string,
	constraintsJSON string,
	response *adminservice.GetDynamicConfigValueResponse,
) string {
	if strings.TrimSpace(constraintsJSON) == "" {
		constraintsJSON = "{}"
	}

	var output strings.Builder
	fmt.Fprintf(&output, "key: %s\n", key)
	writeDynamicConfigYAMLField(&output, "queryConstraints", constraintsJSON)
	fmt.Fprintf(
		&output,
		"constraintDescription: '%s'\n",
		strings.ReplaceAll(formatConstraintDescriptionForYAML(response.GetConstraintDescription()), "'", "''"),
	)
	writeDynamicConfigYAMLField(&output, "effectiveValue", string(response.GetValue()))
	writeDynamicConfigYAMLField(&output, "constrainedValues", string(response.GetConstrainedValues()))
	return output.String()
}

func writeDynamicConfigYAMLField(output *strings.Builder, name string, value string) {
	value = strings.TrimSpace(value)
	if !strings.ContainsRune(value, '\n') {
		fmt.Fprintf(output, "%s: %s\n", name, value)
		return
	}

	fmt.Fprintf(output, "%s:\n", name)
	for line := range strings.SplitSeq(value, "\n") {
		fmt.Fprintf(output, "  %s\n", line)
	}
}

// This only changes diagnostic output from tdbg; server and runtime descriptions remain unchanged.
func formatConstraintDescriptionForYAML(description string) string {
	description = dynamicConfigTaskTypeDescriptionPattern.ReplaceAllString(description, "${1}historyTaskType:")
	return dynamicConfigTaskQueueTypeDescriptionPattern.ReplaceAllString(description, "${1}taskType:")
}

func printDynamicConfigNotes(c *cli.Context, notes ...string) error {
	noteBlock := color.New(color.FgYellow, color.Bold).Sprint(strings.Join(notes, "\n"))
	if _, err := fmt.Fprintf(c.App.ErrWriter, "\n%s\n\n", noteBlock); err != nil {
		return fmt.Errorf("unable to print dynamic config note: %w", err)
	}
	return nil
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
	filename := fmt.Sprintf("tmp_dc_cvs_%s.yaml", uuid.NewString())
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("unable to write dynamic config values to %q: %w", filename, err)
	}
	if _, err = file.Write(response.GetValues()); err != nil {
		_ = file.Close()
		return fmt.Errorf("unable to write dynamic config values to %q: %w", filename, err)
	}
	if err = file.Close(); err != nil {
		return fmt.Errorf("unable to close dynamic config values file %q: %w", filename, err)
	}
	if _, err = fmt.Fprintln(c.App.Writer, filename); err != nil {
		return err
	}
	return nil
}
