package ndc

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestNDCFuncTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(NDCFunctionalTestSuite))
}
