package testvars

import (
	"math"
	"math/rand"
)

func randInt(testHash uint32, hashLen, padLen, randomLen int) int {
	testID := int(testHash) % int(math.Pow10(hashLen))
	pad := int(math.Pow10(padLen + randomLen))
	random := rand.Int() % int(math.Pow10(randomLen))
	return testID*pad + random
}

func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
