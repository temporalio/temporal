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

LoopSwap:
	for i := 0; i < length; i++ {
		index := rand.Intn(i + 1)
		if i == index {
			continue LoopSwap
		}

		result[i], result[index] = result[index], result[i]
	}
	return result
}
