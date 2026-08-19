// Package objectleak tracks heap objects reachable from registered roots and
// reports those that remain after garbage collection. Reports include the
// process-specific heap addresses of unexpected objects so they can be matched
// with a raw heap dump captured from the same process. The package does not
// create heap dumps because they can be large and may contain sensitive data.
package objectleak
