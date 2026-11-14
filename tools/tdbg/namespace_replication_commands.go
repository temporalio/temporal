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
					Description: "Select the namespace with --namespace or --namespace-id. Names are resolved once on the source, then replicas are identified by ID. " +
						"Every name given, including an inherited --namespace or TEMPORAL_CLI_NAMESPACE, must agree with the others and with the ID. " +
						"Run it against the namespace's active cluster; removed peers are not inspected. " +
						"Peers are read through that cluster's frontend, so only --address and its TLS settings are needed.",
					Flags: []cli.Flag{
						&cli.StringFlag{
							Name:  FlagNamespaceID,
							Usage: "Namespace ID; any namespace name given must belong to it",
						},
						&cli.StringFlag{
							Name:    FlagNamespace,
							Aliases: FlagNamespaceAlias,
							Usage:   "Namespace name to resolve on the source, or to cross-check against --namespace-id",
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

// namespaceReplicationInheritedNameSource names where an inherited namespace
// name comes from; urfave/cli does not say whether the flag or the variable set it.
const namespaceReplicationInheritedNameSource = "tdbg --namespace or TEMPORAL_CLI_NAMESPACE"

// namespaceReplicationVerifyRequestFromCLI requires an explicit selector rather
// than tdbg's implicit default namespace. Every namespace name given, on this
// command or inherited from tdbg's --namespace or TEMPORAL_CLI_NAMESPACE, must
// agree; none takes precedence. With an ID, the name is also checked against it.
// Without one, an inherited name selects the namespace.
func namespaceReplicationVerifyRequestFromCLI(c *cli.Context) (namespaceReplicationVerifyRequest, error) {
	id := strings.TrimSpace(c.String(FlagNamespaceID))
	if c.IsSet(FlagNamespaceID) && id == "" {
		return namespaceReplicationVerifyRequest{}, errors.New("--namespace-id is blank")
	}
	// The lineage starts with this command, so a conflict is always between the
	// name given here and an inherited one.
	var name string
	inherited := false
	for i, ancestor := range c.Lineage() {
		if !ancestor.IsSet(FlagNamespace) {
			continue
		}
		candidate := strings.TrimSpace(ancestor.String(FlagNamespace))
		switch {
		case candidate == "" && i > 0:
			// urfave/cli counts an exported but empty TEMPORAL_CLI_NAMESPACE as
			// set. A blank inherited name names no namespace, so it is ignored.
		case candidate == "":
			return namespaceReplicationVerifyRequest{}, errors.New("--namespace is blank; omit it or give the namespace name")
		case name == "":
			name, inherited = candidate, i > 0
		case candidate != name:
			return namespaceReplicationVerifyRequest{}, fmt.Errorf(
				"--namespace %q conflicts with %q from %s",
				name,
				candidate,
				namespaceReplicationInheritedNameSource,
			)
		default:
			// Agrees with the name already seen. Commands between this one and
			// the root have no flag of their own and report the root's value.
		}
	}
	if id == "" && name == "" {
		return namespaceReplicationVerifyRequest{}, errors.New("--namespace or --namespace-id is required")
	}
	return namespaceReplicationVerifyRequest{
		NamespaceID:                    id,
		ExpectedNamespaceName:          name,
		ExpectedNamespaceNameInherited: inherited,
	}, nil
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
