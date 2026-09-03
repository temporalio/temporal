package localexecution

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestValidatePollOptions(t *testing.T) {
	valid := func() *workflowservice.LocalExecutionPollOptions {
		return &workflowservice.LocalExecutionPollOptions{
			LocalServerId:          "local-server",
			ProtocolVersion:        ProtocolVersion,
			SyncInterval:           durationpb.New(5 * time.Second),
			RequestedLeaseDuration: durationpb.New(15 * time.Second),
		}
	}
	require.NoError(t, ValidatePollOptions(valid(), time.Second, time.Minute, 1000))
	require.NoError(t, ValidatePollOptions(nil, time.Second, time.Minute, 1000))

	tests := []struct {
		name   string
		mutate func(*workflowservice.LocalExecutionPollOptions)
	}{
		{
			name: "missing server ID",
			mutate: func(options *workflowservice.LocalExecutionPollOptions) {
				options.LocalServerId = ""
			},
		},
		{
			name: "server ID too long",
			mutate: func(options *workflowservice.LocalExecutionPollOptions) {
				options.LocalServerId = "local-server-id-is-too-long"
			},
		},
		{
			name: "unknown protocol",
			mutate: func(options *workflowservice.LocalExecutionPollOptions) {
				options.ProtocolVersion++
			},
		},
		{
			name: "missing interval",
			mutate: func(options *workflowservice.LocalExecutionPollOptions) {
				options.SyncInterval = nil
			},
		},
		{
			name: "interval below minimum",
			mutate: func(options *workflowservice.LocalExecutionPollOptions) {
				options.SyncInterval = durationpb.New(time.Millisecond)
				options.RequestedLeaseDuration = durationpb.New(3 * time.Millisecond)
			},
		},
		{
			name: "interval above maximum",
			mutate: func(options *workflowservice.LocalExecutionPollOptions) {
				options.SyncInterval = durationpb.New(2 * time.Minute)
				options.RequestedLeaseDuration = durationpb.New(6 * time.Minute)
			},
		},
		{
			name: "missing lease",
			mutate: func(options *workflowservice.LocalExecutionPollOptions) {
				options.RequestedLeaseDuration = nil
			},
		},
		{
			name: "lease is not three intervals",
			mutate: func(options *workflowservice.LocalExecutionPollOptions) {
				options.RequestedLeaseDuration = durationpb.New(10 * time.Second)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid()
			test.mutate(options)
			err := ValidatePollOptions(options, time.Second, time.Minute, 20)
			require.Error(t, err)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}

func TestValidatePollOptionsRejectsInvalidConfiguration(t *testing.T) {
	err := ValidatePollOptions(
		&workflowservice.LocalExecutionPollOptions{
			LocalServerId:          "local-server",
			ProtocolVersion:        ProtocolVersion,
			SyncInterval:           durationpb.New(5 * time.Second),
			RequestedLeaseDuration: durationpb.New(15 * time.Second),
		},
		time.Minute,
		time.Second,
		1000,
	)
	require.Error(t, err)
	require.ErrorAs(t, err, new(*serviceerror.Internal))
}
