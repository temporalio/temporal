//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination size_getter_mock.go

package cache

// SizeGetter is an interface that can be implemented by cache entries to provide their size.
// Cache uses CacheSize() to determine the size of a cache entry.
// Please be aware that if the size of the cache entry changes while the cache is being used without pinning enabled,
// the cache won't be able to automatically adjust for this size change. In such instances, it's necessary to call Put()
// again to ensure the cache size remains accurate.
type (
	SizeGetter interface {
		CacheSize() int
	}
)

func getSize(value interface{}) int {
	if v, ok := value.(SizeGetter); ok {
		return v.CacheSize()
	}
	// if the object does not have a CacheSize() method, assume is count limit cache, which size should be 1
	return 1
}
