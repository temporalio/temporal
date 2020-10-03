package shuffle

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func String(str string) string {
	return string(Bytes([]byte(str)))
}

func Bytes(slice []byte) []byte {
	result := make([]byte, len(slice))
	copy(result, slice)

	rand.Shuffle(len(result), func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	return result
}
