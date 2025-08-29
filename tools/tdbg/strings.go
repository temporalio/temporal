package tdbg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type StringEnum struct {
	Allowed            []string
	Value              string
	ChangedFromDefault bool
}

func NewStringEnum(allowed []string, value string) StringEnum {
	return StringEnum{Allowed: allowed, Value: value}
}

func (s *StringEnum) String() string { return s.Value }

func (s *StringEnum) Set(p string) error {
	for _, allowed := range s.Allowed {
		if p == allowed {
			s.Value = p
			s.ChangedFromDefault = true
			return nil
		}
	}
	return fmt.Errorf("%v is not one of required values of %v", p, strings.Join(s.Allowed, ", "))
}

func (*StringEnum) Type() string { return "string" }

type StringEnumArray struct {
	// maps lower case value to original case
	Allowed map[string]string
	// values in original case
	Values []string
}

func NewStringEnumArray(allowed []string, values []string) StringEnumArray {
	// maps lower case value to original case so we can do case insensitive comparison,
	// while maintaining original case
	var allowedMap = make(map[string]string)
	for _, str := range allowed {
		allowedMap[strings.ToLower(str)] = str
	}

	return StringEnumArray{Allowed: allowedMap, Values: values}
}

func (s *StringEnumArray) String() string { return strings.Join(s.Values, ",") }

func (s *StringEnumArray) Set(p string) error {
	val, ok := s.Allowed[strings.ToLower(p)]
	if !ok {
		values := make([]string, 0, len(s.Allowed))
		for _, v := range s.Allowed {
			values = append(values, v)
		}
		return fmt.Errorf("invalid value: %s, allowed values are: %s", p, strings.Join(values, ", "))
	}
	s.Values = append(s.Values, val)
	return nil
}

func (*StringEnumArray) Type() string { return "string" }

func stringToProtoEnum[T ~int32](s string, maps ...map[string]int32) (T, error) {
	// Go over each map looking, if not there, use first map to build set of
	// strings required
	for _, m := range maps {
		for k, v := range m {
			if strings.EqualFold(k, s) {
				return T(v), nil
			}
		}
	}
	keys := make([]string, 0, len(maps[0]))
	for k := range maps[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return 0, fmt.Errorf("unknown value %q, expected one of: %v", s, strings.Join(keys, ", "))
}

func stringKeysValues(s []string) (map[string]string, error) {
	ret := make(map[string]string, len(s))
	for _, item := range s {
		pieces := strings.SplitN(item, "=", 2)
		if len(pieces) != 2 {
			return nil, fmt.Errorf("missing expected '=' in %q", item)
		}
		ret[pieces[0]] = pieces[1]
	}
	return ret, nil
}

func stringKeysJSONValues(s []string, useJSONNumber bool) (map[string]any, error) {
	if len(s) == 0 {
		return nil, nil
	}
	ret := make(map[string]any, len(s))
	for _, item := range s {
		pieces := strings.SplitN(item, "=", 2)
		if len(pieces) != 2 {
			return nil, fmt.Errorf("missing expected '=' in %q", item)
		}
		dec := json.NewDecoder(bytes.NewReader([]byte(pieces[1])))
		if useJSONNumber {
			dec.UseNumber()
		}
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("invalid JSON value for key %q: %w", pieces[0], err)
		} else if dec.InputOffset() != int64(len(pieces[1])) {
			return nil, fmt.Errorf("invalid JSON value for key %q: unexpected trailing data", pieces[0])
		}
		ret[pieces[0]] = v
	}
	return ret, nil
}
