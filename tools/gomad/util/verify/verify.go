package verify

import "fmt"

func T(predicate bool, msg string, a ...any) {
	if !predicate {
		panic(fmt.Sprintf(msg, a...))
	}
}
