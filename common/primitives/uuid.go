package primitives

import (
	"database/sql/driver"
	"encoding/hex"

	"github.com/google/uuid"
)

// UUID represents a 16-byte universally unique identifier
// this type is a wrapper around google/uuid with the following differences
//  - type is a byte slice instead of [16]byte
//  - db serialization converts uuid to bytes as opposed to string
type UUID []byte

// MustParseUUID returns a UUID parsed from the given string representation
// returns nil if the input is empty string
// panics if the given input is malformed
func MustParseUUID(s string) UUID {
	if s == "" {
		return nil
	}
	u := uuid.MustParse(s)
	return u[:]
}

// Downcast returns the UUID as a byte slice. This is necessary when passing to type sensitive interfaces such as cql.
func (u UUID) Downcast() []byte {
	return u
}

// UUIDPtr simply returns a pointer for the given value type
func UUIDPtr(u UUID) *UUID {
	return &u
}

// String returns the 36 byte hexstring representation of this uuid
// return empty string if this uuid is nil
func (u UUID) String() string {
	if len(u) != 16 {
		return ""
	}
	var buf [36]byte
	u.encodeHex(buf[:])
	return string(buf[:])
}

// StringPtr returns a pointer to the 36 byte hexstring representation of this uuid
// return empty string if this uuid is nil
func (u UUID) StringPtr() *string {
	if len(u) != 16 {
		return stringPtr("")
	}
	var buf [36]byte
	u.encodeHex(buf[:])
	return stringPtr(string(buf[:]))
}

func UUIDString(b []byte) string {
	return UUID(b).String()
}

func stringPtr(v string) *string {
	return &v
}

// Scan implements sql.Scanner interface to allow this type to be
// parsed transparently by database drivers
func (u *UUID) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	guuid := &uuid.UUID{}
	if err := guuid.Scan(src); err != nil {
		return err
	}
	*u = (*guuid)[:]
	return nil
}

// Value implements sql.Valuer so that UUIDs can be written to databases
// transparently. This method returns a byte slice representation of uuid
func (u UUID) Value() (driver.Value, error) {
	return []byte(u), nil
}

func (u UUID) encodeHex(dst []byte) {
	hex.Encode(dst, u[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], u[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], u[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], u[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], u[10:])
}
