package ndc

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestNDCReplicationTaskBatching(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(NDCReplicationTaskBatchingTestSuite))
}
