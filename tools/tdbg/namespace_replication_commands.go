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
					Flags: []cli.Flag{
						&cli.StringFlag{
							Name:  FlagNamespace,
							Usage: "Namespace name",
						},
						&cli.StringFlag{
							Name:  FlagNamespaceID,
							Usage: "Namespace ID",
						},
						&cli.StringSliceFlag{
							Name:  FlagClusterAddress,
							Usage: "Override a target frontend address as <cluster>=<host:port>; may be repeated",
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
	selector, err := namespaceReplicationSelectorFromCLI(c)
	if err != nil {
		return cli.Exit(err.Error(), 2)
	}
	overrides, err := parseNamespaceReplicationAddressOverrides(c.StringSlice(FlagClusterAddress))
	if err != nil {
		return cli.Exit(err.Error(), 2)
	}
	namespaceReplicationFactory, ok := clientFactory.(namespaceReplicationClientFactory)
	if !ok {
		return cli.Exit("configured tdbg client factory does not support per-cluster addresses", 2)
	}

	verifier := newNamespaceReplicationVerifier(cliNamespaceReplicationAdminClientProvider{
		cliContext: c,
		factory:    namespaceReplicationFactory,
	}, options)
	verifier.rpcTimeout = namespaceReplicationRPCTimeout(c)
	result, err := verifier.Verify(c.Context, namespaceReplicationVerifyRequest{
		SourceAddress:    namespaceReplicationFactory.FrontendAddress(c),
		Selector:         selector,
		AddressOverrides: overrides,
	})
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
	timeout := defaultContextTimeout
	if c.IsSet(FlagContextTimeout) {
		timeout = time.Duration(c.Int(FlagContextTimeout)) * time.Second
	}
	return timeout
}

func namespaceReplicationSelectorFromCLI(c *cli.Context) (namespaceReplicationSelector, error) {
	name := strings.TrimSpace(c.String(FlagNamespace))
	id := strings.TrimSpace(c.String(FlagNamespaceID))
	if name == "" && id == "" {
		return namespaceReplicationSelector{}, errors.New("exactly one of --namespace or --namespace-id is required")
	}
	if name != "" && id != "" {
		return namespaceReplicationSelector{}, errors.New("--namespace and --namespace-id are mutually exclusive")
	}
	return namespaceReplicationSelector{Name: name, ID: id}, nil
}

func parseNamespaceReplicationAddressOverrides(values []string) (map[string]string, error) {
	overrides := make(map[string]string, len(values))
	for _, value := range values {
		cluster, address, found := strings.Cut(value, "=")
		cluster = strings.TrimSpace(cluster)
		address = strings.TrimSpace(address)
		if !found || cluster == "" || address == "" {
			return nil, fmt.Errorf("invalid --%s value %q; expected <cluster>=<host:port>", FlagClusterAddress, value)
		}
		if _, exists := overrides[cluster]; exists {
			return nil, fmt.Errorf("duplicate --%s override for cluster %q", FlagClusterAddress, cluster)
		}
		overrides[cluster] = address
	}
	return overrides, nil
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
	if _, err := fmt.Fprintf(
		c.App.Writer,
		"Status: %s\nNamespace: %s (%s)\nSource cluster: %s\n",
		result.Status,
		result.NamespaceName,
		result.NamespaceID,
		result.SourceCluster,
	); err != nil {
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
	Error           string
}

func optionalInt64String(value *int64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatInt(*value, 10)
}
