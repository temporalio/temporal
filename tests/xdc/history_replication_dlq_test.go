package xdc

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestHistoryReplicationDLQSuite(t *testing.T) {
	flag.Parse()
	for _, tc := range []struct {
		name                    string
		enableQueueV2           bool
		enableReplicationStream bool
	}{
		{
			name:                    "QueueV1ReplicationStreamEnabled",
			enableQueueV2:           false,
			enableReplicationStream: true,
		},
		{
			name:                    "QueueV1ReplicationStreamDisabled",
			enableQueueV2:           false,
			enableReplicationStream: false,
		},
		{
			name:                    "QueueV2ReplicationStreamEnabled",
			enableQueueV2:           true,
			enableReplicationStream: true,
		},
		{
			name:                    "QueueV2ReplicationStreamDisabled",
			enableQueueV2:           true,
			enableReplicationStream: false,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s := &historyReplicationDLQSuite{
				enableReplicationStream: tc.enableReplicationStream,
				enableQueueV2:           tc.enableQueueV2,
			}
			suite.Run(t, s)
		})
	}
}
