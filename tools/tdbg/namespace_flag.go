package tdbg

import (
	"flag"

	"github.com/urfave/cli/v2"
)

// namespaceFlag distinguishes an explicit stream constraint from an environment default.
type namespaceFlag struct {
	cli.StringFlag
	explicit bool
}

func (f *namespaceFlag) Apply(set *flag.FlagSet) error {
	if err := f.StringFlag.Apply(set); err != nil {
		return err
	}
	f.explicit = false
	for _, name := range f.Names() {
		value := set.Lookup(name)
		value.Value = &namespaceValue{Value: value.Value, explicit: &f.explicit}
	}
	return nil
}

type namespaceValue struct {
	flag.Value
	explicit *bool
}

func (v *namespaceValue) Set(s string) error {
	if err := v.Value.Set(s); err != nil {
		return err
	}
	*v.explicit = true
	return nil
}

func auditNamespace(c *cli.Context) (string, bool) {
	var fallback string
	for _, ctx := range c.Lineage() {
		if ctx.App == nil {
			continue
		}
		flags := ctx.App.Flags
		if ctx.Command != nil {
			flags = ctx.Command.Flags
		}
		for _, candidate := range flags {
			f, ok := candidate.(*namespaceFlag)
			if !ok {
				continue
			}
			value := ctx.String(FlagNamespace)
			if f.explicit {
				return value, true
			}
			if fallback == "" {
				fallback = value
			}
		}
	}
	return fallback, false
}
