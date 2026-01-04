package tdbg

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
)

type configEntryJSON struct {
	Key         string        `json:"key"`
	Precedence  string        `json:"precedence"`
	Default     string        `json:"default"`
	Values      []configValue `json:"values"`
	Description string        `json:"description"`
}

type configValue struct {
	Constraint string `json:"constraint"`
	Value      string `json:"value"`
}

const (
	FlagKeyFilter = "filter"
	FlagVerbose   = "verbose"
)

func newAdminConfigCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "list",
			Usage: "List all dynamic config settings with their current values",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagKeyFilter,
					Usage: "Filter settings by key prefix (e.g., 'history.' or 'matching.')",
				},
				&cli.BoolFlag{
					Name:    FlagVerbose,
					Aliases: []string{"v"},
					Usage:   "Show full details including descriptions",
				},
				&cli.BoolFlag{
					Name:  "json",
					Usage: "Output as JSON",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminListDynamicConfig(c, clientFactory)
			},
		},
	}
}

// AdminListDynamicConfig lists all dynamic config settings and their configured values.
func AdminListDynamicConfig(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)

	request := &adminservice.GetDynamicConfigRequest{
		KeyFilter: c.String(FlagKeyFilter),
	}

	resp, err := adminClient.GetDynamicConfig(c.Context, request)
	if err != nil {
		return fmt.Errorf("failed to get dynamic config: %w", err)
	}

	entries := resp.GetEntries()

	// Sort entries by key for consistent output
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].GetKey() < entries[j].GetKey()
	})

	if len(entries) == 0 {
		fmt.Fprintln(c.App.Writer, "No dynamic config settings found matching the filter.")
		return nil
	}

	if c.Bool("json") {
		printJSONConfig(c, entries)
		return nil
	}

	if c.Bool(FlagVerbose) {
		printVerboseConfig(c, entries)
		return nil
	}

	printConfigTable(c, entries)
	return nil
}

func printConfigTable(c *cli.Context, entries []*adminservice.DynamicConfigEntry) {
	table := tablewriter.NewWriter(c.App.Writer)
	table.SetHeader([]string{"Key", "Precedence", "Default", "Values"})
	table.SetBorder(false)
	table.SetColumnSeparator("")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderLine(false)
	table.SetAutoWrapText(false)

	for _, entry := range entries {
		def := entry.GetDefaultValue()
		if def == "" {
			def = "(constrained)"
		} else if len(def) > 40 {
			def = def[:37] + "..."
		}

		values := ""
		if cvs := entry.GetConfiguredValues(); len(cvs) > 0 {
			var parts []string
			for _, cv := range cvs {
				constraint := formatConstraints(cv)
				if constraint == "" {
					constraint = "global"
				}
				parts = append(parts, fmt.Sprintf("%s=%s", constraint, cv.GetValue()))
			}
			values = color.YellowString(strings.Join(parts, "; "))
		}

		table.Append([]string{
			entry.GetKey(),
			entry.GetPrecedence(),
			def,
			values,
		})
	}

	fmt.Fprintf(c.App.Writer, "Found %d settings (use -v for details)\n\n", len(entries))
	table.Render()
}

func printJSONConfig(c *cli.Context, entries []*adminservice.DynamicConfigEntry) {
	result := make([]configEntryJSON, 0, len(entries))
	for _, entry := range entries {
		e := configEntryJSON{
			Key:         entry.GetKey(),
			Precedence:  entry.GetPrecedence(),
			Default:     entry.GetDefaultValue(),
			Description: entry.GetDescription(),
			Values:      make([]configValue, 0),
		}
		for _, cv := range entry.GetConfiguredValues() {
			constraint := formatConstraints(cv)
			if constraint == "" {
				constraint = "global"
			}
			e.Values = append(e.Values, configValue{
				Constraint: constraint,
				Value:      cv.GetValue(),
			})
		}
		result = append(result, e)
	}
	b, _ := json.MarshalIndent(result, "", "  ")
	fmt.Fprintln(c.App.Writer, string(b))
}

func printVerboseConfig(c *cli.Context, entries []*adminservice.DynamicConfigEntry) {
	fmt.Fprintf(c.App.Writer, "Found %d dynamic config settings:\n\n", len(entries))

	for _, entry := range entries {
		printConfigEntry(c, entry)
	}
}

func printConfigEntry(c *cli.Context, entry *adminservice.DynamicConfigEntry) {
	// Print key in green
	fmt.Fprintf(c.App.Writer, "%s\n", color.GreenString(entry.GetKey()))

	// Print metadata
	fmt.Fprintf(c.App.Writer, "  Precedence: %s\n", entry.GetPrecedence())

	if entry.GetDefaultValue() != "" {
		fmt.Fprintf(c.App.Writer, "  Default: %s\n", entry.GetDefaultValue())
	} else {
		fmt.Fprintf(c.App.Writer, "  Default: (constrained - multiple defaults based on context)\n")
	}

	// Print description (may be multi-line)
	desc := strings.TrimSpace(entry.GetDescription())
	if desc != "" {
		// Indent multi-line descriptions
		desc = strings.ReplaceAll(desc, "\n", "\n           ")
		fmt.Fprintf(c.App.Writer, "  Description: %s\n", desc)
	}

	// Print configured values
	configuredValues := entry.GetConfiguredValues()
	if len(configuredValues) > 0 {
		fmt.Fprintf(c.App.Writer, "  %s:\n", color.YellowString("Values"))
		for _, cv := range configuredValues {
			constraint := formatConstraints(cv)
			if constraint == "" {
				constraint = "global"
			}
			fmt.Fprintf(c.App.Writer, "    %s=%s\n", constraint, cv.GetValue())
		}
	} else {
		fmt.Fprintf(c.App.Writer, "  Values: (none)\n")
	}

	fmt.Fprintln(c.App.Writer)
}

func formatConstraints(cv *adminservice.DynamicConfigValue) string {
	var parts []string

	if cv.GetNamespace() != "" {
		parts = append(parts, fmt.Sprintf("namespace=%s", cv.GetNamespace()))
	}
	if cv.GetNamespaceId() != "" {
		parts = append(parts, fmt.Sprintf("namespaceID=%s", cv.GetNamespaceId()))
	}
	if cv.GetTaskQueueName() != "" {
		parts = append(parts, fmt.Sprintf("taskQueue=%s", cv.GetTaskQueueName()))
	}
	if cv.GetTaskQueueType() != 0 {
		parts = append(parts, fmt.Sprintf("taskQueueType=%s", cv.GetTaskQueueType().String()))
	}
	if cv.GetShardId() != 0 {
		parts = append(parts, fmt.Sprintf("shardID=%d", cv.GetShardId()))
	}
	if cv.GetTaskType() != 0 {
		parts = append(parts, fmt.Sprintf("taskType=%s", cv.GetTaskType().String()))
	}
	if cv.GetDestination() != "" {
		parts = append(parts, fmt.Sprintf("destination=%s", cv.GetDestination()))
	}

	return strings.Join(parts, ", ")
}
