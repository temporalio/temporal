// freshslicelib exports fresh-result facts for cross-package tests.
package freshslicelib

type Item struct{}

func Build() []*Item { // want Build:`fresh result`
	return []*Item{{}}
}

func PassThrough(items []*Item) []*Item { // want PassThrough:`borrowed result`
	return items
}

func BuildPair() ([]*Item, []*Item) {
	items := []*Item{{}}
	return items, items
}

func BuildIndependentPair() ([]*Item, []*Item) { // want BuildIndependentPair:`fresh result`
	return []*Item{{}}, []*Item{{}}
}
