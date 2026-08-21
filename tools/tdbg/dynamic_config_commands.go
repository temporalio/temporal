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

const dynamicConfigDumpMaxReceiveSize = 16 << 20

var dynamicConfigDumpNote = fmt.Sprintf(
	"Note: This dump contains configured ConstrainedValues only. It does not include registered setting defaults or resolved effective values. Use `tdbg dc get` to query the effective value used by the server. Dump responses are limited to %d MiB; larger responses cause the command to fail without writing a file.",
	dynamicConfigDumpMaxReceiveSize/(1<<20),
)

func newDynamicConfigCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:      "dump",
			Usage:     "Dump all configured dynamic config values",
			UsageText: "tdbg dynamic-config dump [command options]\ntdbg dc dump [command options]",
			Action: func(c *cli.Context) error {
				return dumpDynamicConfigValues(c, clientFactory)
			},
		},
	}
}

func dumpDynamicConfigValues(c *cli.Context, clientFactory ClientFactory) error {
	if _, err := fmt.Fprintln(c.App.ErrWriter, dynamicConfigDumpNote); err != nil {
		return fmt.Errorf("unable to print dynamic config dump note: %w", err)
	}

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
