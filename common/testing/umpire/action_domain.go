package umpire

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrDomainValue       = errors.New("value is outside its domain")
	ErrUnsupportedDomain = errors.New("domain is unsupported")
)

// NormalizingDomain produces invalid neighbors and a canonical non-sensitive comparison value.
type NormalizingDomain interface {
	Domain
	Normalize(any) (string, error)
}

// ValidatorRegistration composes mutation variants with cloning and canonical validation.
type ValidatorRegistration struct {
	Key       string
	Domain    Domain
	Clone     func(any) any
	Normalize func(any) (string, error)
}

// ValidatorRegistry is an immutable lookup table safe for concurrent reads.
type ValidatorRegistry struct {
	entries map[string]ValidatorRegistration
}

// NewValidatorRegistry validates and defensively stores validator registrations.
func NewValidatorRegistry(registrations ...ValidatorRegistration) (*ValidatorRegistry, error) {
	registry := &ValidatorRegistry{entries: make(map[string]ValidatorRegistration, len(registrations))}
	for _, registration := range registrations {
		if registration.Key == "" {
			return nil, fmt.Errorf("%w: validator key is empty", ErrUnsupportedDomain)
		}
		if registration.Domain == nil || registration.Normalize == nil {
			return nil, fmt.Errorf("%w: validator %q is incomplete", ErrUnsupportedDomain, registration.Key)
		}
		if _, duplicate := registry.entries[registration.Key]; duplicate {
			return nil, fmt.Errorf("%w: duplicate validator %q", ErrUnsupportedDomain, registration.Key)
		}
		registry.entries[registration.Key] = registration
	}
	return registry, nil
}

// Domain returns a validator-backed domain, or a typed unsupported lookup error.
func (r *ValidatorRegistry) Domain(key string) (*ValidatorDomain, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: validator registry is nil", ErrUnsupportedDomain)
	}
	registration, found := r.entries[key]
	if !found {
		return nil, fmt.Errorf("%w: no validator registered for %q", ErrUnsupportedDomain, key)
	}
	return &ValidatorDomain{
		base:      registration.Domain,
		clone:     registration.Clone,
		normalize: registration.Normalize,
	}, nil
}

// ValidatorDomain delegates variants to an existing domain and validates a defensive clone.
type ValidatorDomain struct {
	base      Domain
	clone     func(any) any
	normalize func(any) (string, error)
}

func (d *ValidatorDomain) Variants() []Variant {
	if d == nil || d.base == nil {
		return nil
	}
	return slices.Clone(d.base.Variants())
}

func (d *ValidatorDomain) Normalize(value any) (string, error) {
	if d == nil || d.normalize == nil {
		return "", fmt.Errorf("%w: validator domain is incomplete", ErrUnsupportedDomain)
	}
	if d.clone != nil {
		value = d.clone(value)
	}
	normalized, err := d.normalize(value)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrDomainValue, err)
	}
	return normalized, nil
}

// EnumDomain models a finite set of numeric protobuf enum values.
type EnumDomain struct {
	valid   map[int32]struct{}
	invalid int32
}

// NewEnumDomain constructs a finite enum domain and chooses one standard invalid neighbor.
func NewEnumDomain(values []int32) (*EnumDomain, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: enum has no declared values", ErrDomainValue)
	}
	valid := make(map[int32]struct{}, len(values))
	for _, value := range values {
		valid[value] = struct{}{}
	}
	invalid := int32(0)
	for {
		if _, exists := valid[invalid]; !exists {
			break
		}
		if invalid == math.MaxInt32 {
			invalid = math.MinInt32
			for {
				if _, exists := valid[invalid]; !exists {
					break
				}
				invalid++
			}
			break
		}
		invalid++
	}
	return &EnumDomain{valid: valid, invalid: invalid}, nil
}

func (d *EnumDomain) Variants() []Variant {
	return []Variant{{
		Label:  "unknown-enum",
		Class:  OutOfRange,
		Mutate: func(any) any { return d.invalid },
		Expect: &Reject{},
	}}
}

func (d *EnumDomain) Normalize(value any) (string, error) {
	number, ok := enumNumber(value)
	if !ok {
		return "", fmt.Errorf("%w: enum value has type %T", ErrDomainValue, value)
	}
	if _, exists := d.valid[number]; !exists {
		return "", fmt.Errorf("%w: enum number %d", ErrDomainValue, number)
	}
	return strconv.FormatInt(int64(number), 10), nil
}

func enumNumber(value any) (int32, bool) {
	switch number := value.(type) {
	case int32:
		return number, true
	case int:
		if int64(number) < math.MinInt32 || int64(number) > math.MaxInt32 {
			return 0, false
		}
		return int32(number), true
	default:
		return 0, false
	}
}

// IntegerDomain models an inclusive signed integer range.
type IntegerDomain struct {
	min int64
	max int64
}

func NewIntegerDomain(minimum, maximum int64) (*IntegerDomain, error) {
	if minimum > maximum {
		return nil, fmt.Errorf("%w: minimum %d exceeds maximum %d", ErrDomainValue, minimum, maximum)
	}
	return &IntegerDomain{min: minimum, max: maximum}, nil
}

func (d *IntegerDomain) Variants() []Variant {
	var result []Variant
	if d.min > math.MinInt64 {
		result = append(result, Variant{Label: "below-minimum", Class: OutOfRange, Mutate: func(any) any { return d.min - 1 }, Expect: &Reject{}})
	}
	if d.max < math.MaxInt64 {
		result = append(result, Variant{Label: "above-maximum", Class: OutOfRange, Mutate: func(any) any { return d.max + 1 }, Expect: &Reject{}})
	}
	return result
}

func (d *IntegerDomain) Normalize(value any) (string, error) {
	number, ok := signedInteger(value)
	if !ok || number < d.min || number > d.max {
		return "", fmt.Errorf("%w: integer %v is outside [%d,%d]", ErrDomainValue, value, d.min, d.max)
	}
	return strconv.FormatInt(number, 10), nil
}

func signedInteger(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int8:
		return int64(number), true
	case int16:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	default:
		return 0, false
	}
}

// PayloadDomain models payload metadata shape and a bounded encoded size.
type PayloadDomain struct {
	maxBytes int
}

func NewPayloadDomain(maxBytes int) *PayloadDomain {
	return &PayloadDomain{maxBytes: maxBytes}
}

func (d *PayloadDomain) Variants() []Variant {
	return []Variant{
		{
			Label: "missing-encoding", Class: Malformed, Expect: &Reject{},
			Mutate: func(valid any) any {
				payload := clonePayload(valid)
				delete(payload.Metadata, "encoding")
				return payload
			},
		},
		{
			Label: "unknown-encoding", Class: Malformed, Expect: &Reject{},
			Mutate: func(valid any) any {
				payload := clonePayload(valid)
				if payload.Metadata == nil {
					payload.Metadata = map[string][]byte{}
				}
				payload.Metadata["encoding"] = []byte("umpire/unknown")
				return payload
			},
		},
		{
			Label: "too-large", Class: OutOfRange, Expect: &Reject{},
			Mutate: func(valid any) any {
				payload := clonePayload(valid)
				size := d.maxBytes + 1
				if size < 1 {
					size = 1
				}
				payload.Data = make([]byte, size)
				return payload
			},
		},
	}
}

func clonePayload(value any) *commonpb.Payload {
	payload, _ := value.(*commonpb.Payload)
	if payload == nil {
		return &commonpb.Payload{}
	}
	return proto.Clone(payload).(*commonpb.Payload)
}

func (d *PayloadDomain) Normalize(value any) (string, error) {
	payload, ok := value.(*commonpb.Payload)
	if !ok || payload == nil {
		return "", fmt.Errorf("%w: payload has type %T", ErrDomainValue, value)
	}
	if d.maxBytes > 0 && len(payload.GetData()) > d.maxBytes {
		return "", fmt.Errorf("%w: payload has %d bytes, maximum is %d", ErrDomainValue, len(payload.GetData()), d.maxBytes)
	}
	return CanonicalProtoDigest("payload", payload)
}

// CanonicalProtoDigest returns a labeled deterministic digest without retaining serialized data.
func CanonicalProtoDigest(label string, message proto.Message) (string, error) {
	if label == "" {
		return "", fmt.Errorf("%w: digest label is empty", ErrDomainValue)
	}
	if message == nil || !message.ProtoReflect().IsValid() {
		return "", fmt.Errorf("%w: digest message is nil", ErrDomainValue)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("%w: encode %s: %v", ErrDomainValue, label, err)
	}
	digestInput := make([]byte, 0, len(label)+1+len(encoded))
	digestInput = append(digestInput, label...)
	digestInput = append(digestInput, 0)
	digestInput = append(digestInput, encoded...)
	digest := sha256.Sum256(digestInput)
	return fmt.Sprintf("%s:sha256:%x", label, digest), nil
}

// UnsupportedDomain explicitly preserves a dependency-blocked validity domain.
type UnsupportedDomain struct {
	reason string
}

func NewUnsupportedDomain(reason string) *UnsupportedDomain {
	return &UnsupportedDomain{reason: reason}
}

func (*UnsupportedDomain) Variants() []Variant { return nil }

func (d *UnsupportedDomain) Normalize(any) (string, error) {
	return "", fmt.Errorf("%w: %s", ErrUnsupportedDomain, d.reason)
}
