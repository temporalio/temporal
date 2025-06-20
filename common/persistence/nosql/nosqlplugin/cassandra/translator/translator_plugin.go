package translator

import (
	"errors"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common/config"
)

var (
	ErrInvalidTranslatorPluginName = errors.New("translator_plugin: invalid translator plugin requested")
	translators                    = map[string]TranslatorPlugin{}
)

type (
	// TranslatorPlugin interface for Cassandra address translation mechanism
	TranslatorPlugin interface {
		GetTranslator(*config.Cassandra) (gocql.AddressTranslator, error)
	}
)

// RegisterPlugin adds an auth plugin to the plugin registry
// it is only safe to use from a package init function
func RegisterTranslator(name string, plugin TranslatorPlugin) {
	translators[name] = plugin
}

func LookupTranslator(name string) (TranslatorPlugin, error) {
	plugin, ok := translators[name]
	if !ok {
		return nil, ErrInvalidTranslatorPluginName
	}

	return plugin, nil
}
