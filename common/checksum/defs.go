package checksum

import (
	"errors"

	persistenceblobsgenpb "github.com/temporalio/temporal/.gen/proto/persistenceblobs"
)

type (
	// Checksum represents a checksum value along
	// with associated metadata
	Checksum struct {
		// Version represents version of the payload from
		Version int
		// which this checksum was derived
		Flavor Flavor
		// Value is the checksum value
		Value []byte
	}

	// Flavor is an enum type that represents the type of checksum
	Flavor int
)

const (
	// FlavorUnknown represents an unknown/uninitialized checksum flavor
	FlavorUnknown Flavor = iota
	// FlavorIEEECRC32OverProto3Binary represents crc32 checksum generated over proto3 serialized payload
	FlavorIEEECRC32OverProto3Binary
	maxFlavors
)

// ErrMismatch indicates a checksum verification failure due to
// a derived checksum not being equal to expected checksum
var ErrMismatch = errors.New("checksum mismatch error")

// IsValid returns true if the checksum flavor is valid
func (f Flavor) IsValid() bool {
	return f > FlavorUnknown && f < maxFlavors
}

// FromProto returns a new checksum using the proto fields
func FromProto(c *persistenceblobsgenpb.Checksum) *Checksum {
	return &Checksum{
		Version: int(c.Version),
		Flavor:  Flavor(c.Flavor),
		Value:   c.Value,
	}
}

// FromProto returns a new checksum using the proto fields
func (c *Checksum) ToProto() *persistenceblobsgenpb.Checksum {
	return &persistenceblobsgenpb.Checksum{
		Version: int32(c.Version),
		Flavor:  persistenceblobsgenpb.ChecksumFlavor(c.Flavor),
		Value:   c.Value,
	}
}
