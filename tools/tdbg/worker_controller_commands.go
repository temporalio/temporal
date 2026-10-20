package tdbg

import (
	"fmt"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
)

func newAdminWorkerControllerCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "migrate-to-workflow",
			Usage: "Move a CHASM-backed worker controller instance back to its workflow. Close the workflow-to-CHASM migration gate for it first",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: FlagDeploymentName, Usage: "Worker deployment name", Required: true},
				&cli.StringFlag{Name: FlagBuildID, Usage: "Worker deployment build ID", Required: true},
			},
			Action: func(c *cli.Context) error {
				return AdminMigrateWorkerControllerInstance(c, clientFactory)
			},
		},
	}
}

// AdminMigrateWorkerControllerInstance moves one CHASM-backed WCI to its workflow.
func AdminMigrateWorkerControllerInstance(c *cli.Context, clientFactory ClientFactory) error {
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
	}); err != nil {
		return fmt.Errorf("unable to migrate worker controller instance: %w", err)
	}
	_, _ = fmt.Fprintf(c.App.Writer, "Initiated migration of worker controller instance %s:%s in namespace %q to its workflow.\n", deploymentName, buildID, ns)
	return nil
}
