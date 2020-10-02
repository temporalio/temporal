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
	length := len(slice)
	result := make([]byte, 0, length)

	for i := 0; i < length; i++ {
		result = append(result, slice[i])
	}

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
