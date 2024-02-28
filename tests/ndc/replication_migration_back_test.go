package ndc

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestReplicationMigrationBackTest(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(ReplicationMigrationBackTestSuite))

}
