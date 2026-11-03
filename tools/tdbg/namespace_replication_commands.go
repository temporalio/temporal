package tdbg

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

const namespaceReplicationRepairRequiredExitCode = 3

type namespaceReplicationOptions struct {
	dataKeysToIgnore []string
}

func newNamespaceReplicationCommands(
	clientFactory ClientFactory,
	options namespaceReplicationOptions,
	prompterFactory PrompterFactory,
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
				{
					Name:  "repair",
					Usage: "Plan or execute guarded namespace replication repair",
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
						&cli.StringFlag{
							Name:  FlagReason,
							Usage: "Operator reason recorded with every repair write and reconciliation",
						},
						&cli.StringFlag{
							Name:  FlagReconcileBefore,
							Usage: "Only reconcile CHASM local applies resolved at or before this RFC3339 timestamp",
						},
						&cli.BoolFlag{
							Name:  FlagExecute,
							Usage: "Execute the displayed repair plan",
						},
						&cli.BoolFlag{
							Name:  FlagPrintJSON,
							Usage: "Print structured JSON output",
						},
					},
					Action: func(c *cli.Context) error {
						return repairNamespaceReplication(
							c,
							clientFactory,
							options,
							prompterFactory(c),
						)
					},
				},
			},
		},
	}
}

func repairNamespaceReplication(
	c *cli.Context,
	clientFactory ClientFactory,
	options namespaceReplicationOptions,
	prompter *Prompter,
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
	var reconcileBefore *time.Time
	if value := strings.TrimSpace(c.String(FlagReconcileBefore)); value != "" {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return cli.Exit(fmt.Sprintf("invalid --%s: %v", FlagReconcileBefore, err), 2)
		}
		parsed = parsed.UTC()
		reconcileBefore = &parsed
	}
	reason := strings.TrimSpace(c.String(FlagReason))
	if c.Bool(FlagExecute) && reason == "" {
		return cli.Exit("--reason is required with --execute", 2)
	}

	verifier := newNamespaceReplicationVerifier(cliNamespaceReplicationAdminClientProvider{
		cliContext: c,
		factory:    namespaceReplicationFactory,
	}, options)
	verifier.rpcTimeout = namespaceReplicationRPCTimeout(c)
	verifyRequest := namespaceReplicationVerifyRequest{
		SourceAddress:    namespaceReplicationFactory.FrontendAddress(c),
		Selector:         selector,
		AddressOverrides: overrides,
	}
	observation, err := verifier.observe(c.Context, verifyRequest)
	if err != nil {
		return cli.Exit(fmt.Sprintf("verify before repair: %v", err), 2)
	}
	plan, err := planNamespaceReplicationRepair(
		observation,
		uuid.NewString(),
		time.Now().UTC(), // nolint:forbidigo // Operator-visible plan timestamp.
	)
	if err != nil {
		return cli.Exit(err.Error(), 2)
	}
	if plan.Status == namespaceReplicationRepairStatusBlocked {
		if printErr := printNamespaceReplicationRepairPlan(c, plan); printErr != nil {
			return cli.Exit(printErr.Error(), 2)
		}
		return cli.Exit("namespace replication repair is blocked", 2)
	}
	if !c.Bool(FlagExecute) {
		if err := printNamespaceReplicationRepairPlan(c, plan); err != nil {
			return cli.Exit(err.Error(), 2)
		}
		return nil
	}

	if err := printNamespaceReplicationRepairPlanBeforePrompt(c, plan); err != nil {
		return cli.Exit(err.Error(), 2)
	}
	prompt := fmt.Sprintf(
		"Repair namespace %s (%s) from active source %s. Quiesce namespace updates before continuing. Execute repair %s?",
		plan.NamespaceName,
		plan.NamespaceID,
		plan.SourceCluster,
		plan.RepairID,
	)
	if c.Bool(FlagPrintJSON) {
		prompter.prompt(prompt, c.App.ErrWriter)
	} else {
		prompter.Prompt(prompt)
	}
	repairer := newNamespaceReplicationRepairer(verifier)
	result, executeErr := repairer.Execute(c.Context, namespaceReplicationRepairExecutionRequest{
		VerifyRequest:   verifyRequest,
		Observation:     observation,
		Plan:            plan,
		Reason:          reason,
		ReconcileBefore: reconcileBefore,
	})
	if printErr := printNamespaceReplicationRepairExecution(c, result); printErr != nil {
		executeErr = errors.Join(executeErr, printErr)
	}
	if executeErr != nil {
		return cli.Exit(executeErr.Error(), 2)
	}
	return nil
}

func printNamespaceReplicationRepairPlan(
	c *cli.Context,
	plan *namespaceReplicationRepairPlan,
) error {
	return writeNamespaceReplicationRepairPlan(c.App.Writer, plan, c.Bool(FlagPrintJSON))
}

func printNamespaceReplicationRepairPlanBeforePrompt(
	c *cli.Context,
	plan *namespaceReplicationRepairPlan,
) error {
	if c.Bool(FlagYes) {
		return nil
	}
	writer := c.App.Writer
	printJSON := c.Bool(FlagPrintJSON)
	if printJSON {
		// Keep stdout available for the final machine-readable execution result.
		writer = c.App.ErrWriter
		printJSON = false
	}
	return writeNamespaceReplicationRepairPlan(writer, plan, printJSON)
}

func writeNamespaceReplicationRepairPlan(
	writer io.Writer,
	plan *namespaceReplicationRepairPlan,
	printJSON bool,
) error {
	if printJSON {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(plan)
	}
	if _, err := fmt.Fprintf(
		writer,
		"Status: %s\nRepair ID: %s\nNamespace: %s (%s)\nSource cluster: %s\nSource action: %s (%d -> %d)\n",
		plan.Status,
		plan.RepairID,
		plan.NamespaceName,
		plan.NamespaceID,
		plan.SourceCluster,
		plan.SourceAction,
		plan.SourceConfigVersion,
		plan.ProposedSourceConfigVersion,
	); err != nil {
		return err
	}
	if plan.StatusDetail != "" {
		if _, err := fmt.Fprintf(writer, "Detail: %s\n", plan.StatusDetail); err != nil {
			return err
		}
	}
	rows := make([]any, 0, len(plan.Clusters))
	for _, cluster := range plan.Clusters {
		rows = append(rows, namespaceReplicationRepairPlanTableRow{
			Cluster:       cluster.Cluster,
			Presence:      cluster.Presence,
			ConfigVersion: optionalInt64String(cluster.CurrentConfigVersion),
			Action:        cluster.Action,
		})
	}
	return printTable(rows, writer)
}

type namespaceReplicationRepairPlanTableRow struct {
	Cluster       string
	Presence      string
	ConfigVersion string
	Action        string
}

func printNamespaceReplicationRepairExecution(
	c *cli.Context,
	result *namespaceReplicationRepairExecutionResult,
) error {
	if result == nil {
		return errors.New("repair execution returned no result")
	}
	if c.Bool(FlagPrintJSON) {
		encoder := json.NewEncoder(c.App.Writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	if _, err := fmt.Fprintf(
		c.App.Writer,
		"Status: %s\nRepair ID: %s\nNamespace: %s (%s)\nEffective reconciliation cutoff: %s\n",
		result.Status,
		result.Plan.RepairID,
		result.Plan.NamespaceName,
		result.Plan.NamespaceID,
		result.EffectiveCutoff.Format(time.RFC3339Nano),
	); err != nil {
		return err
	}
	if result.StatusDetail != "" {
		if _, err := fmt.Fprintf(c.App.Writer, "Detail: %s\n", result.StatusDetail); err != nil {
			return err
		}
	}
	if result.FinalVerification != nil {
		if _, err := fmt.Fprintf(c.App.Writer, "Final verification: %s\n", result.FinalVerification.Status); err != nil {
			return err
		}
		if result.FinalVerification.StatusDetail != "" {
			if _, err := fmt.Fprintf(c.App.Writer, "Final verification detail: %s\n", result.FinalVerification.StatusDetail); err != nil {
				return err
			}
		}
	}
	rows := []any{namespaceReplicationRepairExecutionTableRow{
		Cluster:  result.Source.Cluster,
		Action:   result.Source.Action,
		Outcome:  result.Source.Outcome,
		Readback: result.Source.Readback,
		Error:    result.Source.Error,
	}}
	for _, target := range result.Targets {
		rows = append(rows, namespaceReplicationRepairExecutionTableRow(target))
	}
	if err := printTable(rows, c.App.Writer); err != nil {
		return err
	}
	if result.ComponentReconciliation == nil {
		return nil
	}
	if len(result.ComponentReconciliation.GetComponents()) == 0 {
		_, err := fmt.Fprintln(c.App.Writer, "Component reconciliation: no running components found")
		return err
	}
	if _, err := fmt.Fprintln(c.App.Writer, "Component reconciliation:"); err != nil {
		return err
	}
	var componentRows []any
	for _, component := range result.ComponentReconciliation.GetComponents() {
		if len(component.GetPeers()) == 0 {
			componentRows = append(componentRows, namespaceReplicationComponentRepairTableRow{
				BusinessID:       component.GetBusinessId(),
				RunID:            component.GetRunId(),
				Eligible:         component.GetEligible(),
				Completed:        component.GetCompleted(),
				IneligibleReason: component.GetIneligibleReason(),
				Error:            component.GetError(),
			})
			continue
		}
		for _, peer := range component.GetPeers() {
			componentRows = append(componentRows, namespaceReplicationComponentRepairTableRow{
				BusinessID:       component.GetBusinessId(),
				RunID:            component.GetRunId(),
				Eligible:         component.GetEligible(),
				Completed:        component.GetCompleted(),
				Peer:             peer.GetTargetCluster(),
				Outcome:          peer.GetOutcome().String(),
				Basis:            peer.GetBasis().String(),
				IneligibleReason: component.GetIneligibleReason(),
				Error:            component.GetError(),
			})
		}
	}
	return printTable(componentRows, c.App.Writer)
}

type namespaceReplicationRepairExecutionTableRow struct {
	Cluster  string
	Action   string
	Outcome  string
	Readback bool
	Error    string
}

type namespaceReplicationComponentRepairTableRow struct {
	BusinessID       string
	RunID            string
	Eligible         bool
	Completed        bool
	Peer             string
	Outcome          string
	Basis            string
	IneligibleReason string
	Error            string
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
