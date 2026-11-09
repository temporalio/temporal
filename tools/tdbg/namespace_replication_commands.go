package tdbg

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
)

const namespaceReplicationRepairRequiredExitCode = 3

type namespaceReplicationOptions struct {
	dataKeysToIgnore []string
}

func newNamespaceReplicationCommands(
	clientFactory ClientFactory,
	options namespaceReplicationOptions,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "replication",
			Usage: "Verify namespace replication state",
			Subcommands: []*cli.Command{
				{
					Name:  "verify",
					Usage: "Compare a global namespace across its configured clusters",
					Description: "Selects the namespace by ID because namespace names can be reused; --namespace only cross-checks it. " +
						"Run it against the namespace's active cluster; removed peers are not inspected. " +
						"Peers are read through that cluster's frontend, so only --address and its TLS settings are needed.",
					Flags: []cli.Flag{
						&cli.StringFlag{
							Name:  FlagNamespaceID,
							Usage: "Namespace ID (required)",
						},
						&cli.StringFlag{
							Name:    FlagNamespace,
							Aliases: FlagNamespaceAlias,
							Usage:   "Optional namespace name to cross-check against --namespace-id; it never selects the namespace, and a --namespace before the subcommand is ignored",
						},
						&cli.BoolFlag{
							Name:  FlagPrintJSON,
							Usage: "Print structured JSON output",
						},
					},
					Action: func(c *cli.Context) error {
						return verifyNamespaceReplication(c, clientFactory, options)
					},
				},
			},
		},
	}
}

func verifyNamespaceReplication(
	c *cli.Context,
	clientFactory ClientFactory,
	options namespaceReplicationOptions,
) error {
	verifyRequest, err := namespaceReplicationVerifyRequestFromCLI(c)
	if err != nil {
		return cli.Exit(err.Error(), 2)
	}
	verifier := newNamespaceReplicationVerifier(adminNamespaceReplicationReader{
		client:     clientFactory.AdminClient(c),
		rpcTimeout: namespaceReplicationRPCTimeout(c),
	}, options)
	result, err := verifier.Verify(c.Context, verifyRequest)
	if err != nil {
		if result != nil {
			if result.StatusDetail != "" {
				err = errors.Join(errors.New(result.StatusDetail), err)
			}
			result.Status = namespaceReplicationStatusBlocked
			result.StatusDetail = err.Error()
			if printErr := printNamespaceReplicationVerification(c, result); printErr != nil {
				return cli.Exit(errors.Join(err, printErr).Error(), 2)
			}
		}
		return cli.Exit(err.Error(), 2)
	}
	if err := printNamespaceReplicationVerification(c, result); err != nil {
		return cli.Exit(err.Error(), 2)
	}

	switch result.Status {
	case namespaceReplicationStatusHealthy:
		return nil
	case namespaceReplicationStatusRepairRequired:
		return cli.Exit("namespace replication repair is required", namespaceReplicationRepairRequiredExitCode)
	default:
		return cli.Exit("namespace replication verification is blocked or inconclusive", 2)
	}
}

func namespaceReplicationRPCTimeout(c *cli.Context) time.Duration {
	return contextTimeout(c, defaultContextTimeout)
}

var errNamespaceReplicationNameSelector = errors.New(
	"--namespace-id is required; a namespace name alone is not accepted because names can be reused",
)

// namespaceReplicationVerifyRequestFromCLI reads the required namespace ID and
// the optional name to cross-check it against. Only a --namespace given on this
// command is cross-checked; an inherited --namespace or TEMPORAL_CLI_NAMESPACE
// is ignored when the ID is supplied, matching how local selectors take
// precedence elsewhere in tdbg.
func namespaceReplicationVerifyRequestFromCLI(c *cli.Context) (namespaceReplicationVerifyRequest, error) {
	id := strings.TrimSpace(c.String(FlagNamespaceID))
	if id == "" {
		for _, ancestor := range c.Lineage() {
			if ancestor.IsSet(FlagNamespace) {
				return namespaceReplicationVerifyRequest{}, errNamespaceReplicationNameSelector
			}
		}
		return namespaceReplicationVerifyRequest{}, errors.New("--namespace-id is required")
	}
	request := namespaceReplicationVerifyRequest{NamespaceID: id}
	if c.IsSet(FlagNamespace) {
		request.ExpectedNamespaceName = strings.TrimSpace(c.String(FlagNamespace))
		if request.ExpectedNamespaceName == "" {
			return namespaceReplicationVerifyRequest{}, errors.New("--namespace is blank; omit it or give the namespace name")
		}
	}
	return request, nil
}

func printNamespaceReplicationVerification(
	c *cli.Context,
	result *namespaceReplicationVerificationResult,
) error {
	if c.Bool(FlagPrintJSON) {
		encoder := json.NewEncoder(c.App.Writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	_, err := fmt.Fprintf(
		c.App.Writer,
		"Status: %s\nNamespace: %s (%s)\nSource cluster: %s\n",
		result.Status,
		result.NamespaceName,
		result.NamespaceID,
		result.SourceCluster,
	)
	if err != nil {
		return err
	}
	if result.StatusDetail != "" {
		if _, err := fmt.Fprintf(c.App.Writer, "Detail: %s\n", result.StatusDetail); err != nil {
			return err
		}
	}

	rows := make([]any, 0, len(result.Clusters))
	for _, cluster := range result.Clusters {
		rows = append(rows, namespaceReplicationVerificationTableRow{
			Cluster:         cluster.Cluster,
			Role:            cluster.Role,
			Presence:        cluster.Presence,
			ConfigVersion:   optionalInt64String(cluster.ConfigVersion),
			ConfigMatch:     cluster.ConfigMatch,
			FailoverVersion: optionalInt64String(cluster.FailoverVersion),
			FailoverMatch:   cluster.FailoverMatch,
			Differences:     strings.Join(cluster.Differences, ","),
			Warnings:        strings.Join(cluster.Warnings, "; "),
			Error:           cluster.Error,
		})
	}
	return printTable(rows, c.App.Writer)
}

type namespaceReplicationVerificationTableRow struct {
	Cluster         string
	Role            string
	Presence        string
	ConfigVersion   string
	ConfigMatch     string
	FailoverVersion string
	FailoverMatch   string
	Differences     string
	Warnings        string
	Error           string
}

func optionalInt64String(value *int64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatInt(*value, 10)
}
