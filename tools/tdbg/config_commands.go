package tdbg

import (
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
)

const (
	FlagKeyFilter = "filter"
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

	fmt.Fprintf(c.App.Writer, "Found %d dynamic config settings:\n\n", len(entries))

	for _, entry := range entries {
		printConfigEntry(c, entry)
	}

	return nil
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

	// Print configured overrides
	configuredValues := entry.GetConfiguredValues()
	if len(configuredValues) > 0 {
		fmt.Fprintf(c.App.Writer, "  %s:\n", color.YellowString("Configured Overrides"))
		for _, cv := range configuredValues {
			constraints := formatConstraints(cv)
			if constraints == "" {
				constraints = "(global)"
			}
			fmt.Fprintf(c.App.Writer, "    %s: %s\n", constraints, cv.GetValue())
		}
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
