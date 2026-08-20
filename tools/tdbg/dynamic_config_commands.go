package tdbg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
)

const dynamicConfigDumpMaxReceiveSize = 128 * 1024 * 1024

func newDynamicConfigCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
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
