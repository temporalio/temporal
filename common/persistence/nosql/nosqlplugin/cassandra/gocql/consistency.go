package gocql

import (
	"fmt"

	"github.com/gocql/gocql"
)

// Definition of all Consistency levels
const (
	Any Consistency = iota
	One
	Two
	Three
	Quorum
	All
	LocalQuorum
	EachQuorum
	LocalOne
)

// Definition of all SerialConsistency levels
const (
	Serial SerialConsistency = iota
	LocalSerial
)

func mustConvertConsistency(c Consistency) gocql.Consistency {
	switch c {
	case Any:
		return gocql.Any
	case One:
		return gocql.One
	case Two:
		return gocql.Two
	case Three:
		return gocql.Three
	case Quorum:
		return gocql.Quorum
	case All:
		return gocql.All
	case LocalQuorum:
		return gocql.LocalQuorum
	case EachQuorum:
		return gocql.EachQuorum
	case LocalOne:
		return gocql.LocalOne
	default:
		panic(fmt.Sprintf("Unknown gocql Consistency level: %v", c))
	}
}
