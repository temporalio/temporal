package tdbg

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestWriteMissedTimes_Table(t *testing.T) {
	s := require.New(t)

	var buf bytes.Buffer
	app := cli.NewApp()
	app.Writer = &buf
	ctx := cli.NewContext(app, nil, nil)

	missed := []missedTime{
		{ScheduleID: "sched-1", ExpectedTime: "2025-01-01T00:00:00Z", Reason: "unknown"},
		{ScheduleID: "sched-2", ExpectedTime: "2025-01-02T12:00:00Z", Reason: "outside catchup window"},
	}

	err := writeMissedTimes(ctx, missed, "table")
	s.NoError(err)

	output := buf.String()
	s.Contains(output, "sched-1")
	s.Contains(output, "sched-2")
	s.Contains(output, "2025-01-01T00:00:00Z")
	s.Contains(output, "2025-01-02T12:00:00Z")
	s.Contains(output, "unknown")
	s.Contains(output, "outside catchup window")
	s.Contains(output, "SCHEDULE ID")
	s.Contains(output, "EXPECTED TIME")
	s.Contains(output, "REASON")
}

func TestWriteMissedTimes_JSONL(t *testing.T) {
	s := require.New(t)

	var buf bytes.Buffer
	app := cli.NewApp()
	app.Writer = &buf
	ctx := cli.NewContext(app, nil, nil)

	missed := []missedTime{
		{ScheduleID: "sched-1", ExpectedTime: "2025-01-01T00:00:00Z", Reason: "unknown"},
		{ScheduleID: "sched-2", ExpectedTime: "2025-01-02T12:00:00Z", Reason: "outside catchup window"},
	}

	err := writeMissedTimes(ctx, missed, "jsonl")
	s.NoError(err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	s.Len(lines, 2)

	for i, line := range lines {
		var got missedTime
		s.NoError(json.Unmarshal([]byte(line), &got))
		s.Equal(missed[i].ScheduleID, got.ScheduleID)
		s.Equal(missed[i].ExpectedTime, got.ExpectedTime)
		s.Equal(missed[i].Reason, got.Reason)
	}
}

func TestWriteMissedTimes_Empty(t *testing.T) {
	s := require.New(t)

	var buf bytes.Buffer
	app := cli.NewApp()
	app.Writer = &buf
	ctx := cli.NewContext(app, nil, nil)

	err := writeMissedTimes(ctx, nil, "table")
	s.NoError(err)
	s.Empty(buf.String())

	err = writeMissedTimes(ctx, nil, "jsonl")
	s.NoError(err)
	s.Empty(buf.String())
}

func TestCollectScheduleIDs_FromFlag(t *testing.T) {
	s := require.New(t)

	var result []string
	app := cli.NewApp()
	app.Commands = []*cli.Command{
		{
			Name: "test-cmd",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagScheduleID,
					Aliases: FlagScheduleIDAlias,
				},
			},
			Action: func(c *cli.Context) error {
				var err error
				result, err = collectScheduleIDs(c)
				return err
			},
		},
	}

	err := app.Run([]string{"app", "test-cmd", "--" + FlagScheduleID, "my-schedule-123"})
	s.NoError(err)
	s.Equal([]string{"my-schedule-123"}, result)
}
