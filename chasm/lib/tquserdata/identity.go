package tquserdata

import (
	"crypto/sha256"
	"encoding/hex"
)

// BusinessID gives user data a stable execution ID separate from workflow IDs.
func BusinessID(taskQueue string) string {
	sum := sha256.Sum256([]byte(taskQueue))
	return "__temporal_tquserdata/" + hex.EncodeToString(sum[:])
}
