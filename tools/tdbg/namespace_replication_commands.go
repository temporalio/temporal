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
					Description: "Selects the namespace by ID because namespace names can be reused. " +
						"Run it against the namespace's active cluster; removed peers are not inspected. " +
						"Peers are read through that cluster's frontend, so only --address and its TLS settings are needed.",
					Flags: []cli.Flag{
						&cli.StringFlag{
							Name:  FlagNamespaceID,
							Usage: "Namespace ID (required)",
						},
						// Hidden so that a namespace name gets an explanatory error
						// instead of an undefined-flag error.
						&cli.StringFlag{
							Name:    FlagNamespace,
							Aliases: FlagNamespaceAlias,
							Hidden:  true,
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
					Description: "Repairs current replicas and reconciles eligible historical CHASM deliveries. " +
						"SUCCEEDED does not imply former replicas were synchronized; deliveries to removed peers are abandoned without inspecting or updating them. " +
						"Selects the namespace by ID; run it against the namespace's active cluster, whose frontend reads and writes peers.",
					Flags: []cli.Flag{
						&cli.StringFlag{
							Name:  FlagNamespaceID,
							Usage: "Namespace ID (required)",
						},
						// Hidden so that a namespace name gets an explanatory error
						// instead of an undefined-flag error.
						&cli.StringFlag{
							Name:    FlagNamespace,
							Aliases: FlagNamespaceAlias,
							Hidden:  true,
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
	namespaceID, err := namespaceReplicationNamespaceIDFromCLI(c)
	if err != nil {
		return cli.Exit(err.Error(), 2)
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

	client := clientFactory.AdminClient(c)
	rpcTimeout := namespaceReplicationRPCTimeout(c)
	verifier := newNamespaceReplicationVerifier(adminNamespaceReplicationReader{
		client:     client,
		rpcTimeout: rpcTimeout,
	}, options)
	verifyRequest := namespaceReplicationVerifyRequest{NamespaceID: namespaceID}
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
	repairer := &namespaceReplicationRepairer{verifier: verifier, client: client, rpcTimeout: rpcTimeout}
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
		return writeJSON(writer, plan)
	}
	_, err := fmt.Fprintf(
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
	)
	if err != nil {
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
			Warnings:      strings.Join(cluster.Warnings, "; "),
		})
	}
	return printTable(rows, writer)
}

type namespaceReplicationRepairPlanTableRow struct {
	Cluster       string
	Presence      string
	ConfigVersion string
	Action        string
	Warnings      string
}

func printNamespaceReplicationRepairExecution(
	c *cli.Context,
	result *namespaceReplicationRepairExecutionResult,
) error {
	if result == nil {
		return errors.New("repair execution returned no result")
	}
	if c.Bool(FlagPrintJSON) {
		return writeJSON(c.App.Writer, result)
	}
	_, err := fmt.Fprintf(
		c.App.Writer,
		"Status: %s\nRepair ID: %s\nNamespace: %s (%s)\nEffective reconciliation cutoff: %s\n",
		result.Status,
		result.Plan.RepairID,
		result.Plan.NamespaceName,
		result.Plan.NamespaceID,
		result.EffectiveCutoff.Format(time.RFC3339Nano),
	)
	if err != nil {
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
	return printNamespaceReplicationComponentReconciliation(c, result)
}

func printNamespaceReplicationComponentReconciliation(c *cli.Context, result *namespaceReplicationRepairExecutionResult) error {
	for _, warning := range result.Warnings {
		if _, err := fmt.Fprintf(c.App.Writer, "Warning: %s\n", warning); err != nil {
			return err
		}
	}
	if result.ComponentReconciliationError != "" {
		if _, err := fmt.Fprintf(c.App.Writer, "Component reconciliation incomplete: %s\n", result.ComponentReconciliationError); err != nil {
			return err
		}
	}
	if result.ComponentReconciliation == nil {
		return nil
	}
	if _, err := fmt.Fprintln(c.App.Writer, result.ComponentDiscoveryNote); err != nil {
		return err
	}
	if len(result.ComponentReconciliation.GetComponents()) == 0 {
		if result.ComponentReconciliationError != "" {
			return nil
		}
		_, err := fmt.Fprintln(c.App.Writer, "Component reconciliation: no running components found in visibility")
		return err
	}
	if _, err := fmt.Fprintln(c.App.Writer, "Component reconciliation:"); err != nil {
		return err
	}
	var componentRows []any
	for _, component := range result.ComponentReconciliation.GetComponents() {
		row := namespaceReplicationComponentRepairTableRow{
			BusinessID: component.GetBusinessId(), RunID: component.GetRunId(), Eligible: component.GetEligible(), Completed: component.GetCompleted(),
			IneligibleReason: component.GetIneligibleReason(), Error: component.GetError(),
		}
		if len(component.GetPeers()) == 0 {
			componentRows = append(componentRows, row)
			continue
		}
		for _, peer := range component.GetPeers() {
			row.Peer, row.Outcome, row.Basis = peer.GetTargetCluster(), peer.GetOutcome().String(), peer.GetBasis().String()
			componentRows = append(componentRows, row)
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
	namespaceID, err := namespaceReplicationNamespaceIDFromCLI(c)
	if err != nil {
		return cli.Exit(err.Error(), 2)
	}
	verifier := newNamespaceReplicationVerifier(adminNamespaceReplicationReader{
		client:     clientFactory.AdminClient(c),
		rpcTimeout: namespaceReplicationRPCTimeout(c),
	}, options)
	result, err := verifier.Verify(c.Context, namespaceReplicationVerifyRequest{NamespaceID: namespaceID})
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

var errNamespaceReplicationNameSelector = errors.New(
	"select the namespace with --namespace-id; namespace names can be reused, so --namespace and TEMPORAL_CLI_NAMESPACE are not accepted",
)

// namespaceReplicationNamespaceIDFromCLI returns the required namespace ID. An
// inherited --namespace or TEMPORAL_CLI_NAMESPACE is ignored when the ID is
// supplied, matching how local selectors take precedence elsewhere in tdbg.
func namespaceReplicationNamespaceIDFromCLI(c *cli.Context) (string, error) {
	if c.IsSet(FlagNamespace) {
		return "", errNamespaceReplicationNameSelector
	}
	if id := strings.TrimSpace(c.String(FlagNamespaceID)); id != "" {
		return id, nil
	}
	for _, parent := range c.Lineage()[1:] {
		if parent.IsSet(FlagNamespace) {
			return "", errNamespaceReplicationNameSelector
		}
	}
	return "", errors.New("--namespace-id is required")
}

func printNamespaceReplicationVerification(
	c *cli.Context,
	result *namespaceReplicationVerificationResult,
) error {
	if c.Bool(FlagPrintJSON) {
		return writeJSON(c.App.Writer, result)
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

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
