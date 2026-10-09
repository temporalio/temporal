package config

import "fmt"

// RPCFaultInjection configures faults uniformly for each transport and direction.
type RPCFaultInjection struct {
	GRPC *TransportFaultInjection `yaml:"grpc"`
	HTTP *TransportFaultInjection `yaml:"http"`
}

// TransportFaultInjection independently configures inbound and outbound calls.
type TransportFaultInjection struct {
	Inbound  *CallFaultInjection `yaml:"inbound"`
	Outbound *CallFaultInjection `yaml:"outbound"`
}

// CallFaultInjection configures faults before a call and after a successful call.
type CallFaultInjection struct {
	Request  FaultInjectionMethodConfig `yaml:"request"`
	Response FaultInjectionMethodConfig `yaml:"response"`
}

func (c RPCFaultInjection) Validate() error {
	for name, cfg := range map[string]*TransportFaultInjection{"grpc": c.GRPC, "http": c.HTTP} {
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("faultInjection.%s: %w", name, err)
		}
	}
	return nil
}

func (c *TransportFaultInjection) Validate() error {
	if c == nil {
		return nil
	}
	for direction, cfg := range map[string]*CallFaultInjection{"inbound": c.Inbound, "outbound": c.Outbound} {
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("%s: %w", direction, err)
		}
	}
	return nil
}

func (c *CallFaultInjection) Validate() error {
	if c == nil {
		return nil
	}
	for stage, cfg := range map[string]FaultInjectionMethodConfig{"request": c.Request, "response": c.Response} {
		for name := range cfg.Errors {
			switch name {
			case "Unavailable", "Internal", "ResourceExhausted":
			default:
				return fmt.Errorf("%s: unsupported transient error %q", stage, name)
			}
		}
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("%s: %w", stage, err)
		}
	}
	return nil
}
