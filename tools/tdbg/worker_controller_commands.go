package tdbg

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/primitives/timestamp"
)

// Each command gets its own flags: urfave/cli keeps parse state on them.
func wciVersionFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: FlagDeploymentName, Usage: "Worker deployment name", Required: true},
		&cli.StringFlag{Name: FlagBuildID, Usage: "Worker deployment build ID", Required: true},
	}
}

func newAdminWorkerControllerCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "migrate-to-workflow",
			Usage: "Move a CHASM-backed worker controller instance back to its workflow. Close the workflow-to-CHASM migration gate for it first",
			Flags: wciVersionFlags(),
			Action: func(c *cli.Context) error {
				return AdminMigrateWorkerControllerInstance(c, clientFactory, adminservice.MigrateWorkerControllerInstanceRequest_TARGET_WORKFLOW)
			},
		},
		{
			Name:  "migrate-to-chasm",
			Usage: "Ask a workflow-backed worker controller instance to hand over to CHASM at its next continue-as-new. Bypasses the migration rollout gate; CHASM routing must be on",
			Flags: wciVersionFlags(),
			Action: func(c *cli.Context) error {
				return AdminMigrateWorkerControllerInstance(c, clientFactory, adminservice.MigrateWorkerControllerInstanceRequest_TARGET_CHASM)
			},
		},
		{
			Name:  "list",
			Usage: "List CHASM-backed worker controller instances with their search attributes. Workflow-backed instances aren't listed",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagVisibilityQuery,
					Aliases: FlagVisibilityQueryAlias,
					Usage: "Visibility query over WorkerControllerInstanceStatus, WCIValidationStatus, WCIComputeProviders, " +
						"WorkerDeploymentName, WorkerDeploymentVersion. Defaults to running instances",
				},
				&cli.IntFlag{Name: FlagPageSize, Value: 100, Usage: "Page size"},
				&cli.BoolFlag{Name: FlagMore, Usage: "Prompt for further pages"},
				&cli.BoolFlag{Name: FlagPrintJSON, Usage: "Print raw JSON"},
			},
			Action: func(c *cli.Context) error {
				return AdminListWorkerControllerInstances(c, clientFactory)
			},
		},
	}
}

type compactWCIInfo struct {
	InstanceID        string
	Status            string
	Validation        string
	Providers         string
	DeploymentVersion string
	StartTime         time.Time
	CloseTime         *time.Time
}

// AdminListWorkerControllerInstances lists CHASM WCIs in a namespace.
func AdminListWorkerControllerInstances(c *cli.Context, clientFactory ClientFactory) error {
	ns, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	pageSize := c.Int(FlagPageSize)
	if pageSize <= 0 {
		return errors.New("page size must be positive")
	}
	tableView := !c.Bool(FlagPrintJSON)
	client := clientFactory.AdminClient(c)
	req := &adminservice.ListWorkerControllerInstancesRequest{
		Namespace: ns,
		Query:     c.String(FlagVisibilityQuery),
		PageSize:  int32(pageSize),
	}
	paginationFunc := func(token []byte) ([]any, []byte, error) {
		ctx, cancel := newContext(c)
		defer cancel()
		req.NextPageToken = token
		resp, err := client.ListWorkerControllerInstances(ctx, req)
		if err != nil {
			return nil, nil, err
		}
		var items []any
		for _, wci := range resp.GetInstances() {
			if !tableView {
				items = append(items, wci)
				continue
			}
			items = append(items, compactWCIInfo{
				InstanceID:        wci.GetInstanceId(),
				Status:            wci.GetStatus(),
				Validation:        wci.GetValidationStatus(),
				Providers:         strings.Join(wci.GetComputeProviders(), ","),
				DeploymentVersion: wci.GetDeploymentVersion(),
				StartTime:         wci.GetStartTime().AsTime(),
				CloseTime:         timestamp.TimeValuePtr(wci.GetCloseTime()),
			})
		}
		return items, resp.GetNextPageToken(), nil
	}
	if err := paginate(c, paginationFunc, pageSize); err != nil {
		return fmt.Errorf("unable to list worker controller instances: %w", err)
	}
	return nil
}

// AdminMigrateWorkerControllerInstance moves one WCI to the target backend.
func AdminMigrateWorkerControllerInstance(c *cli.Context, clientFactory ClientFactory, target adminservice.MigrateWorkerControllerInstanceRequest_Target) error {
	ns, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	deploymentName, buildID := c.String(FlagDeploymentName), c.String(FlagBuildID)

	ctx, cancel := newContext(c)
	defer cancel()
	if _, err := clientFactory.AdminClient(c).MigrateWorkerControllerInstance(ctx, &adminservice.MigrateWorkerControllerInstanceRequest{
		Namespace:      ns,
		DeploymentName: deploymentName,
		BuildId:        buildID,
		Identity:       getCurrentUserFromEnv(),
		Target:         target,
	}); err != nil {
		return fmt.Errorf("unable to migrate worker controller instance: %w", err)
	}
	backend := "its workflow"
	if target == adminservice.MigrateWorkerControllerInstanceRequest_TARGET_CHASM {
		backend = "CHASM"
	}
	_, _ = fmt.Fprintf(c.App.Writer, "Initiated migration of worker controller instance %s:%s in namespace %q to %s.\n", deploymentName, buildID, ns, backend)
	return nil
}
