package elasticsearch

import (
	"go.temporal.io/server/tools/common/schema/test"
)

type (
	SetupSchemaTestSuite struct {
		test.SetupSchemaTestBase
	}
)

func (s *SetupSchemaTestSuite) TestSetupSchema() {
	// Integration test similar to cassandra - this would require a real ES instance
	// For now, just test that the CLI tool can be created
	app := BuildCLIOptions()
	s.NotNil(app)
}
