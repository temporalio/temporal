package tdbg

import (
	"fmt"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
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
	}
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
