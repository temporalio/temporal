package xdc

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestFuncClustersTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(FunctionalClustersTestSuite))
}
