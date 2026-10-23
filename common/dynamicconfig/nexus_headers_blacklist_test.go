package dynamicconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestFrontendNexusRequestHeadersBlacklistDefault(t *testing.T) {
	re := dynamicconfig.FrontendNexusRequestHeadersBlacklist.Get(dynamicconfig.NewNoopCollection())()

	for _, header := range []string{
		"accept-encoding",
		"temporal-nexus-failure-support",
		"temporal-internal-foo",
	} {
		assert.True(t, re.MatchString(header), header)
	}
	for _, header := range []string{
		"user-header",
		"temporal-internal",
	} {
		assert.False(t, re.MatchString(header), header)
	}
}
