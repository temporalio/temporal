// The MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination auth_plugin_mock.go
package auth

import (
	"fmt"

	"go.temporal.io/server/common/config"
)

var (
	ErrInvalidAuthPluginName = fmt.Errorf("auth_plugin: invalid auth plugin requested")
	plugins                  = map[string]AuthPlugin{}
)

type (
	// AuthPlugin interface for mutating SQL connection parameters
	AuthPlugin interface {
		// GetConfig returns a mutated SQL config
		GetConfig(*config.SQL) (*config.SQL, error)
	}
)

func RegisterPlugin(name string, plugin AuthPlugin) {
	plugins[name] = plugin
}

func DeregisterPlugin(name string) {
	delete(plugins, name)
}

func LookupPlugin(name string) (AuthPlugin, error) {
	plugin, ok := plugins[name]
	if !ok {
		return nil, ErrInvalidAuthPluginName
	}

	return plugin, nil
}
