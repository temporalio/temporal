package tdbg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"google.golang.org/grpc"
)

const dynamicConfigDumpMaxReceiveSize = 128 * 1024 * 1024

func newDynamicConfigCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "get",
			Usage: "Get the effective value of one dynamic config key",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagDynamicConfigKey,
					Usage:    "Dynamic config key",
					Required: true,
				},
				&cli.StringFlag{
					Name:  FlagNamespaceID,
					Usage: "Namespace ID filter",
				},
				&cli.StringFlag{
					Name:  FlagTaskQueue,
					Usage: "Task queue name filter",
				},
				&cli.StringFlag{
					Name:  FlagTaskQueueType,
					Usage: "Task queue type filter",
				},
				&cli.IntFlag{
					Name:  FlagShardID,
					Usage: "Shard ID filter",
				},
				&cli.StringFlag{
					Name:  FlagTaskType,
					Usage: "History task type filter",
				},
				&cli.StringFlag{
					Name:  FlagDestination,
					Usage: "Destination filter",
				},
				&cli.StringFlag{
					Name:  FlagChasmTaskType,
					Usage: "CHASM task type filter",
				},
			},
			Action: func(c *cli.Context) error {
				return getDynamicConfigValue(c, clientFactory)
			},
		},
		{
			Name:  "dump",
			Usage: "Dump dynamic config data",
			Subcommands: []*cli.Command{
				{
					Name:  "cvs",
					Usage: "Dump all constrained values held by the dynamic config client",
					Action: func(c *cli.Context) error {
						return dumpDynamicConfigValues(c, clientFactory)
					},
				},
			},
		},
	}
}

func getDynamicConfigValue(c *cli.Context, clientFactory ClientFactory) error {
	taskQueueType, err := StringToEnum(c.String(FlagTaskQueueType), enumspb.TaskQueueType_value)
	if err != nil {
		return fmt.Errorf("invalid task queue type: %w", err)
	}
	taskType, err := StringToEnum(c.String(FlagTaskType), enumsspb.TaskType_value)
	if err != nil {
		return fmt.Errorf("invalid task type: %w", err)
	}

	ctx, cancel := newContext(c)
	defer cancel()
	response, err := clientFactory.AdminClient(c).GetDynamicConfigValue(
		ctx,
		&adminservice.GetDynamicConfigValueRequest{
			Key:           c.String(FlagDynamicConfigKey),
			Namespace:     c.String(FlagNamespace),
			NamespaceId:   c.String(FlagNamespaceID),
			TaskQueue:     c.String(FlagTaskQueue),
			TaskQueueType: enumspb.TaskQueueType(taskQueueType),
			ShardId:       int32(c.Int(FlagShardID)),
			TaskType:      enumsspb.TaskType(taskType),
			Destination:   c.String(FlagDestination),
			ChasmTaskType: c.String(FlagChasmTaskType),
		},
	)
	if err != nil {
		return fmt.Errorf("unable to get dynamic config value: %w", err)
	}
	_, err = fmt.Fprintln(c.App.Writer, string(response.GetValue()))
	return err
}

func dumpDynamicConfigValues(c *cli.Context, clientFactory ClientFactory) error {
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := clientFactory.AdminClient(c).DumpDynamicConfigValues(
		ctx,
		&adminservice.DumpDynamicConfigValuesRequest{},
		grpc.MaxCallRecvMsgSize(dynamicConfigDumpMaxReceiveSize),
	)
	if err != nil {
		return fmt.Errorf("unable to dump dynamic config values: %w", err)
	}

	var output bytes.Buffer
	if err := json.Indent(&output, response.GetValues(), "", "  "); err != nil {
		return fmt.Errorf("unable to format dynamic config values: %w", err)
	}
	if err := output.WriteByte('\n'); err != nil {
		return fmt.Errorf("unable to format dynamic config values: %w", err)
	}

	filename := fmt.Sprintf("tmp_dc_cvs_%s.json", time.Now().UTC().Format("20060102T150405Z"))
	if err := os.WriteFile(filename, output.Bytes(), 0o644); err != nil {
		return fmt.Errorf("unable to write dynamic config values to %q: %w", filename, err)
	}
	_, err = fmt.Fprintln(c.App.Writer, filename)
	return err
}
